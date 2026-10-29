#!/usr/bin/env python3
"""AST-level resume-site lint for the Flow headers.

Walks a clang -ast-dump (text format) of the flow probe TU and flags
every coroutine resumption whose ENCLOSING declaration context is not in
the allowlist. Detected shapes:

  D1  MemberExpr `.resume` whose RECEIVER subtree is coroutine_handle-
      typed (receiver-confirmed — a foreign type's `.resume()` is not
      flagged)
  D2  CXXOperatorCallExpr whose OBJECT argument (second child) is
      coroutine_handle-typed — the `h()` spelling; a functor merely
      TAKING a handle parameter is not flagged
  D3  CXXDependentScopeMemberExpr `.resume` (template-dependent — how
      offload's deliver_drop_unbound dumps)
  D4  std::invoke / std::invoke_r calls with a coroutine_handle-typed
      argument (indirect laundering)
  D5  member-pointer forms: any expression whose type is a pointer-to-
      member OF coroutine_handle (`::*` + coroutine_handle), and `.*` /
      `->*` operator calls with a coroutine_handle receiver

Scope is the SDK HEADER TREE BY PATH (any tracked file whose path
contains the aorta include dir — never a filename pattern): a new
header in the tree is in scope automatically, and an in-tree helper
cannot hide. Out-of-tree laundering cannot ship (shipped headers live in
the tree); std-internal instantiation bodies attribute to std headers
and are the RECORDED residual — the canary covers runtime detection.

Contexts are FULLY QUALIFIED. Template-decl folding is KIND-AWARE: only
a ClassTemplateDecl/FunctionTemplateDecl immediately wrapping its
same-named inner decl folds — adjacent same-name NAMESPACES do not
(otherwise a nested aorta::aorta namespace would bypass the allowlist).
Scope names parse from the text AFTER the location segment, so path
tokens never leak in.

Supplementary text scan: comment-stripped, word-boundary CALL
TOKEN (`sym(`) counts over EVERY header under the scanned dir
(recursive) — nonzero-without-pin and stale pins both fail.

Usage: flow_resume_lint.py <ast-dump.txt> <allowlist.txt> <header-dir>
Exit 0 clean; 1 on violations (message names the offending context).
"""

import re
import sys
from pathlib import Path

NODE_RE = re.compile(r"^([| `]*[|`]-)(\w+)\s?(.*)$")
DECL_KINDS = {"FunctionDecl", "CXXMethodDecl", "CXXConstructorDecl",
              "CXXConversionDecl", "CXXDestructorDecl"}
SCOPE_KINDS = {"NamespaceDecl", "CXXRecordDecl", "ClassTemplateDecl",
               "FunctionTemplateDecl"}
TEMPLATE_KINDS = {"ClassTemplateDecl", "FunctionTemplateDecl"}
NAME_BEFORE_TYPE_RE = re.compile(r"([\w~]+|operator\(\))(?=\s+')")
SCOPE_TAIL_SKIP = {"definition", "struct", "class", "union", "inline",
                   "implicit", "referenced", "used", "nested"}
FILE_RE = re.compile(r"([^\s<>(),']+\.(?:h|hpp|hh|cc|cpp)):\d+")
POST_SYMBOLS = ["aorta_executor_post_resume_after",
                "aorta_executor_post_resume",
                "post_resume_marker", "post_resume_member",
                "post_resume_item", "post_resume_class"]
# Path-prefix scope: the aorta SDK header tree (works for repo-relative
# paths, bazel runfiles, and the red probes' fake tree alike).
# RECURSIVE tree scope: anything under an aorta
# include dir, at any nesting depth, is in scope.
SCOPE_PATH_RE = re.compile(r"(?:^|/)(?:cpp/)?include/aorta/.+\.(?:h|hpp)$"
                           r"|(?:^|/)aorta/.+\.(?:h|hpp)$")
CORO_TYPE = "coroutine_handle"
MEMBER_PTR_RE = re.compile(r"coroutine_handle[^']*::\s*\*"
                           r"|::\*[^']*coroutine_handle")


def scope_name(rest):
    """Scope decl name, parsed from the text AFTER the location segment
    (path tokens can never leak in); None for
    anonymous/implicit scopes (folded into the parent)."""
    tail = rest.rsplit(">", 1)[-1] if ">" in rest else rest
    tail = tail.split("'")[0]
    toks = re.findall(r"[A-Za-z_~]\w*|operator\(\)", tail)
    for t in reversed(toks):
        if t in SCOPE_TAIL_SKIP or t in ("line", "col"):
            continue
        return t
    return None


def strip_comments(text):
    out, i, n = [], 0, len(text)
    while i < n:
        if text.startswith("//", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
        elif text.startswith("/*", i):
            j = text.find("*/", i + 2)
            i = n if j < 0 else j + 2
        elif text[i] in "\"'":
            q = text[i]
            out.append(text[i]); i += 1
            while i < n and text[i] != q:
                if text[i] == "\\":
                    out.append(text[i]); i += 1
                if i < n:
                    out.append(text[i]); i += 1
            if i < n:
                out.append(text[i]); i += 1
        else:
            out.append(text[i]); i += 1
    return "".join(out)


def count_call_tokens(text, sym):
    return len(re.findall(r"\b" + re.escape(sym) + r"\s*\(", text))


FIRST_TYPE_RE = re.compile(r"'([^']*)'")
CORO_LEADING_RE = re.compile(
    r"^(?:const\s+)?(?:std::(?:__\d+::)?)?coroutine_handle\b")
# Pointer/reference-to-handle receiver forms (hp->resume()): the
# receiver root's type is 'std::coroutine_handle<...> *' — still
# leading-token coroutine_handle, so CORO_LEADING_RE covers it.


def line_type_is_coro(rest):
    """True when the line's OWN (first-quoted) type is coroutine_handle
    at the TOP level — a template ARGUMENT mention (Foreign<handle>,
    Sink<handle>) does not count."""
    m = FIRST_TYPE_RE.search(rest)
    return bool(m and CORO_LEADING_RE.match(m.group(1).strip()))


class Pending:
    """A node whose verdict needs its SUBTREE: buffered until the walk
    pops back to its depth. Direct children are POSITION-tracked —
    receiver/object/argument confirmation reads the direct child's OWN
    top-level type, never arbitrary subtree text."""

    def __init__(self, depth, shape, ctx, detail):
        self.depth = depth
        self.shape = shape          # 'member' | 'opcall' | 'invoke'
        self.ctx = ctx
        self.detail = detail
        self.child_no = 0           # direct children seen (depth+1)
        self.callee_operator = False
        self.object_is_coro = False
        self.receiver_is_coro = False
        self.arg_is_coro = False
        self.is_invoke = False
        self.nested_call_depth = None  # a call node inside child #1

    def feed(self, depth, kind, rest):
        direct = depth == self.depth + 1
        if direct:
            self.child_no += 1
        if self.shape == "member":
            # Confirm on the receiver ROOT (the
            # member expr's DIRECT child) only — a handle buried in a
            # receiver-constructing call (make_foreign(h).resume()) is
            # not the receiver.
            if direct and line_type_is_coro(rest):
                self.receiver_is_coro = True
        elif self.shape == "opcall":
            if self.child_no == 1 and "CXXMethod" in rest \
                    and "operator()" in rest:
                self.callee_operator = True
            if direct and self.child_no == 2 \
                    and line_type_is_coro(rest):
                self.object_is_coro = True
        elif self.shape == "invoke":
            # D4 contract: invoke must be THIS call's CALLEE — inside
            # child #1 but with NO intervening call node
            # (choose(std::invoke(...))(h) nests an invoke
            # inside the callee subtree without being the callee) — and
            # the handle one of THIS call's own direct arguments
            # (children >= 2).
            if self.child_no == 1:
                if self.nested_call_depth is not None \
                        and depth <= self.nested_call_depth:
                    self.nested_call_depth = None
                if kind.endswith("CallExpr") \
                        and self.nested_call_depth is None:
                    self.nested_call_depth = depth
                if self.nested_call_depth is None \
                        and "DeclRefExpr" in kind \
                        and "Function" in rest and re.search(
                            r"'invoke(?:_r)?'", rest):
                    self.is_invoke = True
            if direct and self.child_no >= 2 \
                    and line_type_is_coro(rest):
                self.arg_is_coro = True

    def verdict(self):
        if self.shape == "member":
            return self.receiver_is_coro
        if self.shape == "opcall":
            return self.callee_operator and self.object_is_coro
        if self.shape == "invoke":
            return self.is_invoke and self.arg_is_coro
        return False


def parse_dump(path):
    """Yield (qualified_context, shape, detail) for flagged resumptions
    inside the path-scoped SDK header tree."""
    stack = []           # (depth, name, kind)
    hits = []
    cur_file = ""
    pending = []         # list[Pending]
    with open(path, "r", errors="replace") as f:
        for line in f:
            m = NODE_RE.match(line)
            if not m:
                continue
            depth = len(m.group(1)) // 2
            kind, rest = m.group(2), m.group(3)
            fm = None
            for fm_ in FILE_RE.finditer(rest):
                fm = fm_
            if fm:
                cur_file = fm.group(1)
            in_scope = bool(SCOPE_PATH_RE.search(cur_file))
            while stack and stack[-1][0] >= depth:
                stack.pop()
            while pending and depth <= pending[-1].depth:
                p = pending.pop()
                if p.verdict():
                    hits.append((p.ctx, p.shape, p.detail))
            for p in pending:
                p.feed(depth, kind, rest)
            if kind in DECL_KINDS:
                nm = NAME_BEFORE_TYPE_RE.search(rest)
                if nm:
                    name = nm.group(1)
                    if stack and stack[-1][1] == "(lambda)" \
                            and name == "operator()":
                        pass  # fold into the (lambda) context
                    elif (stack and stack[-1][1] == name
                          and stack[-1][2] in TEMPLATE_KINDS):
                        pass  # FunctionTemplateDecl + inner same-name fn
                    else:
                        stack.append((depth, name, kind))
            elif kind in SCOPE_KINDS:
                name = scope_name(rest)
                if name is None:
                    pass  # anonymous/implicit: fold into parent
                elif (stack and stack[-1][1] == name
                      and stack[-1][2] in TEMPLATE_KINDS):
                    pass  # ClassTemplateDecl + its same-name record ONLY
                          # (adjacent same-name NAMESPACES both push)
                else:
                    stack.append((depth, name, kind))
            elif kind == "LambdaExpr":
                stack.append((depth, "(lambda)", kind))
            elif not in_scope:
                continue
            elif kind == "MemberExpr" and re.search(
                    r"(?:\.|->)(?:resume\b|operator\(\))", rest):
                # Arrow access (hp->resume()) and
                # the explicit h.operator()() spelling (a plain
                # CXXMemberCallExpr, not CXXOperatorCallExpr) both
                # count; receiver confirmation below applies the same.
                pending.append(Pending(depth, "member", qual(stack),
                                       rest.strip()[:80]))
            elif kind == "CXXDependentScopeMemberExpr" \
                    and re.search(r"(?:\.|->)resume\b", rest):
                hits.append((qual(stack), "dependent-call",
                             rest.strip()[:80]))
            elif kind == "CXXOperatorCallExpr":
                pending.append(Pending(depth, "opcall", qual(stack),
                                       rest.strip()[:80]))
            elif kind == "CallExpr":
                pending.append(Pending(depth, "invoke", qual(stack),
                                       rest.strip()[:80]))
            elif MEMBER_PTR_RE.search(rest):
                # D5: any in-scope expression carrying a pointer-to-
                # member-of-coroutine_handle type (taking &::resume,
                # storing it, or a .*/->* apply).
                hits.append((qual(stack), "member-pointer",
                             rest.strip()[:80]))
    while pending:
        p = pending.pop()
        if p.verdict():
            hits.append((p.ctx, p.shape, p.detail))
    return hits


def qual(stack):
    return "::".join(n for _, n, _ in stack) or "<file-scope>"


def main():
    dump, allow_file, hdr_dir = sys.argv[1], sys.argv[2], sys.argv[3]
    allowed = set()
    count_pins = {}
    for raw in Path(allow_file).read_text().splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        if s.startswith("countpin "):
            _, fname, sym, n = s.split()
            count_pins[(fname, sym)] = int(n)
        else:
            allowed.add(s)

    hits = parse_dump(dump)
    seen = set()
    violations = []
    for ctx, shape, detail in hits:
        key = (ctx, shape)
        if key in seen:
            continue  # template instantiations duplicate AST nodes
        seen.add(key)
        if ctx not in allowed:
            violations.append((ctx, f"{shape}: {detail}"))

    observed = {}
    root = Path(hdr_dir)
    for hdr in sorted(root.rglob("*.h*")):
        rel = hdr.relative_to(root).as_posix()
        # RELATIVE-PATH keys — a nested same-basename header cannot
        # shadow a root pin.
        text = strip_comments(hdr.read_text(errors="replace"))
        for sym in POST_SYMBOLS:
            n = count_call_tokens(text, sym)
            if n:
                observed[(rel, sym)] = n
    for key, n in sorted(observed.items()):
        pinned = count_pins.get(key)
        if pinned != n:
            violations.append((
                f"countpin:{key[0]}",
                f"{key[1]}( occurs {n}x, pinned "
                f"{'nothing' if pinned is None else pinned} — a post site "
                f"changed; re-audit the resume-site inventory, then update "
                f"the pin"))
    for key, pinned in sorted(count_pins.items()):
        if key not in observed:
            violations.append((
                f"countpin:{key[0]}",
                f"stale pin: {key[1]}( pinned {pinned}x but occurs 0x"))

    if violations:
        print("flow resume lint FAILED — resumption outside the routed "
              "inventory:")
        for ctx, detail in violations:
            print(f"  [{ctx}] {detail}")
        print("Route the resume through detail::route_resume, or extend "
              "cpp/lint/flow_resume_allowlist.txt IN THE SAME PR that "
              "documents the new resume site.")
        return 1
    print(f"flow resume lint clean ({len(seen)} distinct allowlisted "
          f"resume contexts; {len(observed)} count-pinned post sites)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
