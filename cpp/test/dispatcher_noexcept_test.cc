// Compile-time contract assertions for the four `extern "C"`
// dispatcher functions and TypedClientResponse's move-assign
// operator.
//
// Each `static_assert(noexcept(...))` here turns the noexcept
// contract into a build break that surfaces the instant a future
// edit drops the specifier — much louder than waiting for an
// implicit unwind to escape an `extern "C"` boundary at runtime
// and abort the process. The test binary is otherwise empty; if
// it links, the contract holds.

#include <gtest/gtest.h>

#include "aorta/contextstore.h"
#include "aorta/subscriber.h"
#include "aorta/service.h"
#include "aorta/timer.h"
#include "aorta/typed_service.h"
#include "aorta/types.h"

namespace {

// Every dispatcher swallows exceptions in its body; marking them
// `noexcept` makes that contract
// part of the signature. extern "C" Rust callers are `nounwind`
// since rust 1.81 so an uncaught exception would already terminate;
// the static_assert catches the regression at build time.
//
// The `aorta::` qualifier matters: even though these are `extern
// "C"` (C linkage on the linker side), the C++ declarations live
// inside `namespace aorta { ... }` in the headers and definitions
// in the .cc files. C linkage only affects symbol mangling, not
// name lookup, so from this file's anonymous namespace the
// unqualified names aren't visible.
static_assert(
    noexcept(aorta::aorta_cpp_on_msg(
        static_cast<const aorta_bytes_view_t*>(nullptr),
        static_cast<const aorta_schema_hash_view_t*>(nullptr),
        static_cast<const aorta_msg_context_t*>(nullptr),
        static_cast<void*>(nullptr))),
    "aorta_cpp_on_msg must be noexcept");

static_assert(
    noexcept(aorta::aorta_cpp_on_service_request(
        static_cast<const aorta_bytes_view_t*>(nullptr),
        aorta_service_request_t{0},
        static_cast<void*>(nullptr))),
    "aorta_cpp_on_service_request must be noexcept");

static_assert(
    noexcept(aorta::aorta_cpp_on_timer(static_cast<void*>(nullptr))),
    "aorta_cpp_on_timer must be noexcept");

static_assert(
    noexcept(aorta::aorta_cpp_on_ctx_change(
        static_cast<const aorta_ctx_change_t*>(nullptr),
        static_cast<void*>(nullptr))),
    "aorta_cpp_on_ctx_change must be noexcept");

// TypedClientResponse's move-assign operator must be
// noexcept so that container code (vector::push_back of move-only
// types, exception-safety guarantees of `swap`, etc.) doesn't pay
// the "may throw" tax. Every member is independently noexcept-
// moveable (the underlying ClientResponse holds a TBytes payload
// and a Status, both noexcept-moveable); the operator was just
// missing the explicit specifier.

struct DummyMsg {};

static_assert(
    std::is_nothrow_move_assignable<aorta::TypedClientResponse<DummyMsg>>::value,
    "TypedClientResponse::operator=(TypedClientResponse&&) must be noexcept");

}  // namespace

// gtest needs at least one test case in the binary so the
// generated `main` symbol resolves; the real assertions are at
// compile time above.
TEST(DispatcherNoexceptContract, CompileTimeAssertionsHold) {
  SUCCEED();
}
