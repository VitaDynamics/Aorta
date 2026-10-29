# Third-party license texts

Verbatim upstream license texts for third-party work that Aorta redistributes
in **source** form. These are notice-preservation obligations: they attach to
this repository, to the exported Cargo release workspace, and to any binary
built from either. Do not edit the license files — they are byte-verbatim
copies of upstream.

| File | Upstream | Covers |
|---|---|---|
| [`foxglove-sdk.LICENSE`](foxglove-sdk.LICENSE) | [foxglove/foxglove-sdk](https://github.com/foxglove/foxglove-sdk) `LICENSE` (MIT) — `Copyright (c) Foxglove Technologies Inc` | The 18 derived schemas in `message/schemas/topic/foxglove_schema/`, and every generated message crate that embeds one of them (including the vendored `Time.fbs` copies). |

The `aorta_msgs` Python wheel ships the same file under its
`.dist-info/licenses/THIRD_PARTY_LICENSES/` directory, next to a list of the
installed modules derived from foxglove-sdk schemas.

Full policy: [`docs/licensing.md`](../docs/licensing.md).
