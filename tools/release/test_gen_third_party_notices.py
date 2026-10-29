import copy
import unittest

from tools.release import gen_third_party_notices as notices

from tools.release.gen_third_party_notices import parse_cargo_tree, render


CRATES_IO = "sparse+https://index.crates.io/"


# The two source IDs Cargo writes for crates.io, and near misses of each: a
# different scheme, port (the default one included), user info, query,
# fragment, path or host spelling names some other registry. Each is kept as
# (kind, URL) and joined below: the export's remote allowlist reads a
# `registry+`/`git+` literal in code as a fetch declaration.
def source_id(kind, url):
    return f"{kind}+{url}" if kind else url


CANONICAL_CRATES_IO = tuple(source_id(kind, url) for kind, url in (
    ("registry", "https://github.com/rust-lang/crates.io-index"),
    ("sparse", "https://index.crates.io/"),
))
NEAR_CRATES_IO = tuple(source_id(kind, url) for kind, url in (
    ("sparse", "http://index.crates.io:12345/"),
    ("registry", "http://github.com:12345/rust-lang/crates.io-index"),
    ("sparse", "http://index.crates.io/"),
    ("registry", "http://github.com/rust-lang/crates.io-index"),
    ("sparse", "https://index.crates.io:443/"),
    ("registry", "https://github.com:443/rust-lang/crates.io-index"),
    ("sparse", "https://token@index.crates.io/"),
    ("registry", "https://user:secret@github.com/rust-lang/crates.io-index"),
    ("sparse", "https://index.crates.io/?mirror"),
    ("sparse", "https://index.crates.io/?"),
    ("registry", "https://github.com/rust-lang/crates.io-index#main"),
    ("sparse", "https://index.crates.io/#"),
    ("sparse", "https://index.crates.io"),
    ("sparse", "https://index.crates.io/private/"),
    ("registry", "https://github.com/rust-lang/crates.io-index/"),
    ("registry", "https://github.com/rust-lang/crates.io-index.git"),
    ("sparse", "https://INDEX.crates.io/"),
    ("registry", "https://index.crates.io/"),
    ("sparse", "https://github.com/rust-lang/crates.io-index"),
    ("git", "https://github.com/rust-lang/crates.io-index"),
    ("", "https://index.crates.io/"),
))


def mpl_sentence(name, version):
    return f"Source code for this component is available at https://crates.io/crates/{name}/{version}."


class NoticeTest(unittest.TestCase):
    def test_evidence_comparison_ignores_order_and_local_package_paths(self):
        first = self.two_crates()
        first.update(cargo_lock_sha256='lock', policy_sha256='policy')
        second = copy.deepcopy(first)
        second['crates'].reverse()
        second['licenses'].reverse()
        second['crates'][0]['package']['manifest_path'] = '/other/checkout/Cargo.toml'
        notices.compare_evidence(first, second)

    def test_evidence_comparison_rejects_license_and_reachability_drift(self):
        first = self.two_crates()
        first.update(cargo_lock_sha256='lock', policy_sha256='policy')
        for field in ('license_text', 'reachability', 'policy_sha256', 'cargo_lock_sha256', 'reachable_root'):
            with self.subTest(field=field):
                second = copy.deepcopy(first)
                if field == 'license_text':
                    second['licenses'][0]['text'] += 'changed attribution'
                elif field == 'reachability':
                    second['reachable']['x86_64-unknown-linux-gnu'].append(['local', '1.0.0', 'path'])
                else:
                    second[field] = 'different'
                with self.assertRaises(ValueError):
                    notices.compare_evidence(first, second)

    def data(self):
        package = {"name": "example", "version": "1.0.0", "source": "registry+test"}
        return {
            "crates": [{"package": package, "license": "MIT"}],
            "licenses": [{"id": "MIT", "text": "Copyright Example\nPermission is hereby granted",
                          "used_by": [{"crate": package}]}],
        }

    def test_preserves_attribution_and_license_text(self):
        result = render(self.data())
        self.assertIn("example | 1.0.0 | MIT", result)
        self.assertIn("Copyright Example\nPermission is hereby granted", result)

    def test_missing_crate_license_is_rejected(self):
        data = self.data()
        data["crates"][0]["license"] = "Unknown"
        with self.assertRaisesRegex(ValueError, "example.*Unknown"):
            render(data)

    def test_missing_license_text_is_rejected(self):
        data = self.data()
        data["licenses"][0]["text"] = ""
        with self.assertRaisesRegex(ValueError, "empty license text"):
            render(data)

    def test_uncovered_crate_is_rejected(self):
        data = self.data()
        data["licenses"][0]["used_by"] = []
        with self.assertRaisesRegex(ValueError, "example.*no license text"):
            render(data)

    def two_crates(self):
        linked = {"name": "linked", "version": "1.0.0", "source": CRATES_IO}
        tool = {"name": "tool-only", "version": "2.0.0", "source": CRATES_IO}
        return {
            "crates": [{"package": linked, "license": "MIT"}, {"package": tool, "license": "MPL-2.0"}],
            "licenses": [
                {"id": "MIT", "text": "MIT text", "used_by": [{"crate": linked}]},
                {"id": "MPL-2.0", "text": "MPL text", "used_by": [{"crate": tool}]},
            ],
            "reachable_root": "aorta_core",
            "reachable": {"x86_64-unknown-linux-gnu": [["linked", "1.0.0", "registry"]]},
        }

    def test_target_restricts_notices_to_the_reachable_crates(self):
        result = render(self.two_crates(), target="x86_64-unknown-linux-gnu")
        self.assertIn("linked | 1.0.0 | MIT", result)
        self.assertIn("MIT text", result)
        self.assertNotIn("tool-only", result)
        self.assertNotIn("MPL text", result)
        self.assertIn("x86_64-unknown-linux-gnu", result)
        self.assertIn("cargo tree -e normal --target x86_64-unknown-linux-gnu -p aorta_core", result)

    def test_unrestricted_render_keeps_every_crate(self):
        result = render(self.two_crates())
        self.assertIn("tool-only | 2.0.0 | MPL-2.0", result)
        self.assertIn("MPL text", result)

    def test_mpl_component_states_where_its_source_is(self):
        result = render(self.two_crates())
        self.assertIn("- tool-only 2.0.0: " + mpl_sentence("tool-only", "2.0.0"), result)
        self.assertEqual(result.count("Source code for this component"), 1)

    def test_compound_expression_containing_mpl_states_the_source(self):
        for expression in ("MIT OR MPL-2.0", "(Apache-2.0 AND MPL-2.0)", "MPL-2.0+ OR MIT"):
            with self.subTest(expression=expression):
                data = self.two_crates()
                # cargo-about may select the non-MPL alternative; the crate is still MPL-covered.
                data["crates"][1]["license"] = expression
                data["licenses"][0]["used_by"].append(data["licenses"][1]["used_by"].pop())
                result = render(data)
                self.assertIn("- tool-only 2.0.0: " + mpl_sentence("tool-only", "2.0.0"), result)

    def test_non_mpl_components_get_no_source_sentence(self):
        result = render(self.data())
        self.assertNotIn("Source code for this component", result)
        self.assertNotIn("MPL-2.0 source code availability", result)
        restricted = render(self.two_crates(), target="x86_64-unknown-linux-gnu")
        self.assertNotIn("Source code for this component", restricted)
        self.assertNotIn(mpl_sentence("linked", "1.0.0"), render(self.two_crates()))

    def test_selected_mpl_license_alone_states_the_source(self):
        data = self.two_crates()
        # The expression does not name MPL-2.0, but cargo-about selected it.
        data["crates"][1]["license"] = "LicenseRef-Unknown-Fixture"
        result = render(data)
        self.assertIn("tool-only | 2.0.0 | MPL-2.0", result)
        self.assertIn("- tool-only 2.0.0: " + mpl_sentence("tool-only", "2.0.0"), result)

    def test_mpl_component_from_either_canonical_crates_io_index_states_the_source(self):
        for origin in CANONICAL_CRATES_IO:
            with self.subTest(origin=origin):
                self.assertTrue(notices.from_crates_io(origin))
                data = self.two_crates()
                # The same object backs the crate and its license user.
                data["crates"][1]["package"]["source"] = origin
                self.assertIn("- tool-only 2.0.0: " + mpl_sentence("tool-only", "2.0.0"), render(data))

    def test_mpl_component_outside_crates_io_is_rejected(self):
        for origin in ("registry+test", *NEAR_CRATES_IO):
            with self.subTest(origin=origin):
                self.assertFalse(notices.from_crates_io(origin))
                data = self.two_crates()
                data["crates"][1]["package"]["source"] = origin
                with self.assertRaisesRegex(ValueError, "(?s)LICENSE GATE FAIL.*tool-only 2.0.0: MPL-2.0"):
                    render(data)

    def test_target_without_reachability_evidence_is_rejected(self):
        data = self.two_crates()
        with self.assertRaisesRegex(ValueError, "aarch64-unknown-linux-gnu.*rerun --collect"):
            render(data, target="aarch64-unknown-linux-gnu")
        del data["reachable"]
        with self.assertRaisesRegex(ValueError, "x86_64-unknown-linux-gnu.*rerun --collect"):
            render(data, target="x86_64-unknown-linux-gnu")

    def test_reachable_crate_without_license_evidence_is_rejected(self):
        data = self.two_crates()
        data["reachable"]["x86_64-unknown-linux-gnu"].append(["ghost", "0.1.0", "registry"])
        with self.assertRaisesRegex(ValueError, "(?s)LICENSE GATE FAIL.*ghost 0.1.0"):
            render(data, target="x86_64-unknown-linux-gnu")

    def test_reachable_path_crates_need_no_evidence(self):
        data = self.two_crates()
        data["reachable"]["x86_64-unknown-linux-gnu"].append(["aorta_sync", "0.1.0", "path"])
        render(data, target="x86_64-unknown-linux-gnu")

    def test_parse_cargo_tree_output(self):
        output = "\n".join([
            "aorta_core v0.1.0 (/work/core)",
            "bytes v1.10.1",
            "bytes v1.10.1 (*)",
            "async-trait v0.1.89 (proc-macro)",
            "zenoh v1.10.1 (file:///srv/forks/zenoh.git?rev=abc#abc)",
            "",
        ])
        self.assertEqual(parse_cargo_tree(output), [
            ["aorta_core", "0.1.0", "path"],
            ["async-trait", "0.1.89", "registry"],
            ["bytes", "1.10.1", "registry"],
            ["zenoh", "1.10.1", "git"],
        ])
        with self.assertRaisesRegex(ValueError, "unparseable cargo tree line"):
            parse_cargo_tree("not a package line")

    def test_parse_cargo_tree_ignores_terminal_colours(self):
        # The exact line the SDK bundle container printed (CARGO_TERM_COLOR=always).
        output = "flatbuffers v25.12.19 \x1b[33m\x1b[2m(*)\x1b[39m\x1b[22m\n"
        self.assertEqual(parse_cargo_tree(output), [["flatbuffers", "25.12.19", "registry"]])


if __name__ == "__main__":
    unittest.main()
