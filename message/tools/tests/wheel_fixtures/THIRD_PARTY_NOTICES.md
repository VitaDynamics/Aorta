# Third-party notices (test fixture)

Stand-in for the THIRD_PARTY_NOTICES.md the release job renders from
cargo-about evidence (`tools/release/gen_third_party_notices.py --target`).
It lets the wheel composition test run without that evidence; the manual
release test checks the real notices. Not a release artifact.
