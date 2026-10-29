use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

enum ArchiveEntry {
    File(PathBuf),
    Symlink(String),
    ThirdPartyLock(PathBuf),
    Generated(Vec<u8>),
}

/// `lib/libaorta_core.so.<N>`: the Linux Core member carries the ABI generation
/// in its name, matching the DT_SONAME the linker stamped from the same source.
fn core_shared_object_name(core_abi: u32) -> String {
    format!("libaorta_core.so.{core_abi}")
}

fn archive_path(kind: &str, source: &Path, core_abi: u32) -> Result<String, String> {
    let source_text = source.to_string_lossy().replace('\\', "/");
    let basename = source
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("invalid source path: {}", source.display()))?;
    let path = match kind {
        "cpp_header" => {
            let marker = "cpp/include/";
            let offset = source_text
                .find(marker)
                .ok_or_else(|| format!("C++ header is outside cpp/include: {source_text}"))?;
            format!("include/{}", &source_text[offset + marker.len()..])
        }
        "ffi_header" => "include/aorta.h".to_owned(),
        "generated_header" => format!("include/{basename}"),
        "cmake" => format!("share/cmake/Aorta/{basename}"),
        "flatbuffers_header" => {
            let marker = "include/flatbuffers/";
            let offset = source_text.find(marker).ok_or_else(|| {
                format!("FlatBuffers header is outside include/flatbuffers: {source_text}")
            })?;
            format!(
                "include/flatbuffers/{}",
                &source_text[offset + marker.len()..]
            )
        }
        "flatbuffers_cmake" => format!("share/cmake/FlatBuffers/{basename}"),
        "core_shared" => {
            if basename.ends_with(".dylib") {
                "lib/libaorta_core.dylib".to_owned()
            } else if basename.ends_with(".so") {
                format!("lib/{}", core_shared_object_name(core_abi))
            } else if basename.ends_with(".dll") {
                "lib/aorta_core.dll".to_owned()
            } else {
                return Err(format!("unsupported Core shared library: {basename}"));
            }
        }
        "cpp_static" => "lib/libaorta_cpp.a".to_owned(),
        "cpp_shared" => {
            if basename.ends_with(".dylib") {
                "lib/libaorta_cpp.dylib".to_owned()
            } else if basename.ends_with(".so") {
                "lib/libaorta_cpp.so".to_owned()
            } else if basename.ends_with(".dll") {
                "lib/aorta_cpp.dll".to_owned()
            } else {
                return Err(format!("unsupported C++ shared library: {basename}"));
            }
        }
        "legal" => basename.to_owned(),
        "third_party_lock" => "THIRD_PARTY_COMPONENTS.md".to_owned(),
        _ => return Err(format!("unknown entry kind: {kind}")),
    };
    Ok(path)
}

fn write_octal(field: &mut [u8], value: u64) -> Result<(), String> {
    let width = field.len();
    let text = format!("{:0width$o}\0", value, width = width - 1);
    if text.len() != width {
        return Err(format!("tar numeric field overflow: {value}"));
    }
    field.copy_from_slice(text.as_bytes());
    Ok(())
}

fn write_tar_header<W: Write>(
    writer: &mut W,
    name: &str,
    size: usize,
    type_flag: u8,
    link_name: &str,
) -> Result<(), String> {
    let mut header = [0u8; 512];
    let (prefix, leaf) = if name.len() <= 100 {
        ("", name)
    } else {
        let split = name[..name.len().min(255)]
            .rfind('/')
            .ok_or_else(|| format!("tar member path is too long: {name}"))?;
        (&name[..split], &name[split + 1..])
    };
    if leaf.len() > 100 || prefix.len() > 155 {
        return Err(format!("tar member path is too long: {name}"));
    }
    header[..leaf.len()].copy_from_slice(leaf.as_bytes());
    header[345..345 + prefix.len()].copy_from_slice(prefix.as_bytes());
    write_octal(&mut header[100..108], 0o644)?;
    write_octal(&mut header[108..116], 0)?;
    write_octal(&mut header[116..124], 0)?;
    write_octal(&mut header[124..136], size as u64)?;
    write_octal(&mut header[136..148], 0)?;
    header[148..156].fill(b' ');
    header[156] = type_flag;
    if link_name.len() > 100 {
        return Err(format!("tar link path is too long: {link_name}"));
    }
    header[157..157 + link_name.len()].copy_from_slice(link_name.as_bytes());
    header[257..263].copy_from_slice(b"ustar\0");
    header[263..265].copy_from_slice(b"00");
    let checksum: u64 = header.iter().map(|byte| u64::from(*byte)).sum();
    let checksum_text = format!("{:06o}\0 ", checksum);
    header[148..156].copy_from_slice(checksum_text.as_bytes());

    writer.write_all(&header).map_err(io_error)
}

fn write_tar_entry<W: Write>(writer: &mut W, name: &str, data: &[u8]) -> Result<(), String> {
    write_tar_header(writer, name, data.len(), b'0', "")?;
    writer.write_all(data).map_err(io_error)?;
    let padding = (512 - (data.len() % 512)) % 512;
    writer.write_all(&vec![0u8; padding]).map_err(io_error)?;
    Ok(())
}

fn write_tar_symlink<W: Write>(writer: &mut W, name: &str, target: &str) -> Result<(), String> {
    write_tar_header(writer, name, 0, b'2', target)
}

fn io_error(error: io::Error) -> String {
    error.to_string()
}

/// One `MANIFEST.json` row: what the member is and what it must hash to.
enum ManifestEntry {
    File { sha256: String, size: usize },
    Symlink { target: String },
}

/// The bundle contract includes SHA-256 checksums. The out-of-band
/// `<archive>.tar.zst.sha256` sidecar only proves the archive arrived intact;
/// per-member digests let a consumer (and the release gate) verify what was
/// unpacked, so tampering with a single header after extraction is detectable.
fn manifest_json(members: &BTreeMap<String, ManifestEntry>) -> Vec<u8> {
    let entries = members
        .iter()
        .map(|(path, entry)| match entry {
            ManifestEntry::File { sha256, size } => {
                format!("    {{\"path\": \"{path}\", \"sha256\": \"{sha256}\", \"size\": {size}}}")
            }
            ManifestEntry::Symlink { target } => {
                format!("    {{\"path\": \"{path}\", \"symlink\": \"{target}\"}}")
            }
        })
        .collect::<Vec<_>>()
        .join(",\n");
    format!("{{\n  \"format_version\": 2,\n  \"members\": [\n{entries}\n  ]\n}}\n").into_bytes()
}

/// The ABI generation is read from a file rendered by //core:core_abi out of
/// `AORTA_ABI_VERSION`, so this tool never restates the number.
fn read_core_abi(path: &Path) -> Result<u32, String> {
    let text = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    text.trim()
        .parse()
        .map_err(|error| format!("invalid Core ABI in {}: {error}", path.display()))
}

/// The bundle records the exact source commit and Core ABI it was
/// built from. An unstamped developer build records an empty commit.
fn provenance_json(aorta_commit: &str, core_abi: u32) -> Vec<u8> {
    format!("{{\"aorta_commit\": \"{aorta_commit}\", \"core_abi\": {core_abi}}}\n").into_bytes()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn cargo_lock_value(block: &str, key: &str) -> Option<String> {
    let prefix = format!("{key} = \"");
    block.lines().find_map(|line| {
        line.strip_prefix(&prefix)
            .and_then(|value| value.strip_suffix('"'))
            .map(str::to_owned)
    })
}

fn third_party_components(lock_path: &Path) -> Result<Vec<u8>, String> {
    let lock = fs::read_to_string(lock_path)
        .map_err(|error| format!("failed to read {}: {error}", lock_path.display()))?;
    let mut components = Vec::new();
    for block in lock.split("[[package]]").skip(1) {
        let Some(source) = cargo_lock_value(block, "source") else {
            continue;
        };
        let name = cargo_lock_value(block, "name").ok_or("Cargo.lock package is missing name")?;
        let version =
            cargo_lock_value(block, "version").ok_or("Cargo.lock package is missing version")?;
        let checksum = cargo_lock_value(block, "checksum").unwrap_or_else(|| "-".to_owned());
        components.push((name, version, source, checksum));
    }
    components.sort();

    let mut output = String::from(
        "# Third-Party Components (SBOM Seed)\n\n\
Generated from the workspace `Cargo.lock`. This inventory is not a license grant, \
license opinion, or complete SBOM. The P0 legal gate remains authoritative.\n\n\
| Component | Version | Source | Cargo checksum |\n\
|---|---:|---|---|\n",
    );
    for (name, version, source, checksum) in components {
        output.push_str(&format!(
            "| `{name}` | `{version}` | `{source}` | `{checksum}` |\n"
        ));
    }
    Ok(output.into_bytes())
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let output = PathBuf::from(args.next().ok_or("missing archive output")?);
    let members_output = PathBuf::from(args.next().ok_or("missing members output")?);
    let mut aorta_commit = None;
    let mut core_abi = None;
    let mut members = Vec::new();
    for argument in args {
        let (kind, value) = argument
            .split_once('=')
            .ok_or_else(|| format!("entry must be KIND=VALUE: {argument}"))?;
        match kind {
            "aorta_commit" => aorta_commit = Some(value.to_owned()),
            "core_abi" => core_abi = Some(read_core_abi(Path::new(value))?),
            _ => members.push((kind.to_owned(), PathBuf::from(value))),
        }
    }
    let aorta_commit = aorta_commit.ok_or("missing aorta_commit=<sha or empty>")?;
    let core_abi = core_abi.ok_or("missing core_abi=<rendered ABI file>")?;
    if members.is_empty() {
        return Err("release archive has no members".to_owned());
    }

    let mut entries = BTreeMap::new();
    entries.insert(
        "PROVENANCE.json".to_owned(),
        ArchiveEntry::Generated(provenance_json(&aorta_commit, core_abi)),
    );
    for (kind, source) in members {
        let destination = archive_path(&kind, &source, core_abi)?;
        let entry = if kind == "third_party_lock" {
            ArchiveEntry::ThirdPartyLock(source)
        } else {
            ArchiveEntry::File(source)
        };
        if entries.insert(destination.clone(), entry).is_some() {
            return Err(format!("duplicate archive member: {destination}"));
        }
        if kind == "core_shared"
            && destination == format!("lib/{}", core_shared_object_name(core_abi))
        {
            let symlink = "lib/libaorta_core.so".to_owned();
            if entries
                .insert(
                    symlink.clone(),
                    ArchiveEntry::Symlink(core_shared_object_name(core_abi)),
                )
                .is_some()
            {
                return Err(format!("duplicate archive member: {symlink}"));
            }
        }
    }
    // Materialize every member first so MANIFEST.json can carry the digest of
    // the exact bytes that are written into the archive below.
    let mut payloads: BTreeMap<String, Option<Vec<u8>>> = BTreeMap::new();
    let mut manifest_entries: BTreeMap<String, ManifestEntry> = BTreeMap::new();
    for (destination, entry) in entries {
        match entry {
            ArchiveEntry::File(source) => {
                let data = fs::read(&source)
                    .map_err(|error| format!("failed to read {}: {error}", source.display()))?;
                manifest_entries.insert(
                    destination.clone(),
                    ManifestEntry::File {
                        sha256: sha256_hex(&data),
                        size: data.len(),
                    },
                );
                payloads.insert(destination, Some(data));
            }
            ArchiveEntry::ThirdPartyLock(source) => {
                let data = third_party_components(&source)?;
                manifest_entries.insert(
                    destination.clone(),
                    ManifestEntry::File {
                        sha256: sha256_hex(&data),
                        size: data.len(),
                    },
                );
                payloads.insert(destination, Some(data));
            }
            ArchiveEntry::Generated(data) => {
                manifest_entries.insert(
                    destination.clone(),
                    ManifestEntry::File {
                        sha256: sha256_hex(&data),
                        size: data.len(),
                    },
                );
                payloads.insert(destination, Some(data));
            }
            ArchiveEntry::Symlink(target) => {
                manifest_entries.insert(
                    destination.clone(),
                    ManifestEntry::Symlink {
                        target: target.clone(),
                    },
                );
                payloads.insert(destination, None);
            }
        }
    }

    let manifest = manifest_json(&manifest_entries);
    let mut all_members = vec!["MANIFEST.json".to_owned()];
    all_members.extend(payloads.keys().cloned());
    all_members.sort();
    fs::write(&members_output, all_members.join("\n") + "\n").map_err(io_error)?;

    let output_file = File::create(output).map_err(io_error)?;
    let mut encoder = zstd::stream::write::Encoder::new(output_file, 19).map_err(io_error)?;
    write_tar_entry(&mut encoder, "MANIFEST.json", &manifest)?;
    for (destination, payload) in payloads {
        match payload {
            Some(data) => write_tar_entry(&mut encoder, &destination, &data)?,
            None => {
                let ManifestEntry::Symlink { target } = &manifest_entries[&destination] else {
                    return Err(format!("missing symlink target for {destination}"));
                };
                write_tar_symlink(&mut encoder, &destination, target)?;
            }
        }
    }
    encoder.write_all(&[0u8; 1024]).map_err(io_error)?;
    encoder.finish().map_err(io_error)?;
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("bundle_archive: {error}");
        std::process::exit(1);
    }
}
