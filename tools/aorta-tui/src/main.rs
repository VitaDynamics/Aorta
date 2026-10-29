#[derive(Debug, PartialEq, Eq)]
enum CommandLine {
    Run {
        group: Option<String>,
        no_client_override: bool,
    },
    Help,
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<CommandLine, String> {
    let mut group = None;
    let mut no_client_override = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--group" => {
                let Some(value) = args.next() else {
                    return Err("Missing value for --group".to_string());
                };
                if value.starts_with('-') {
                    return Err(format!("Missing value for --group before flag: {value}"));
                }
                group = Some(value);
            }
            "--no-client-override" => {
                no_client_override = true;
            }
            "--help" | "-h" => {
                return Ok(CommandLine::Help);
            }
            other => {
                return Err(format!("Unknown argument: {other}"));
            }
        }
    }
    Ok(CommandLine::Run {
        group,
        no_client_override,
    })
}

fn print_help() {
    eprintln!("Usage: aorta-tui [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("      --group <GROUP>      Aorta group");
    eprintln!("      --no-client-override  Do not override Zenoh session mode to client.");
    eprintln!("                            By default the TUI connects in client mode for");
    eprintln!("                            faster startup. Future: use a dedicated client-mode");
    eprintln!("                            config file instead.");
    eprintln!("  -h, --help               Print help");
}

fn main() -> anyhow::Result<()> {
    match parse_args(std::env::args().skip(1)) {
        Ok(CommandLine::Run {
            group,
            no_client_override,
        }) => aorta_tui::run(group.as_deref(), !no_client_override),
        Ok(CommandLine::Help) => {
            print_help();
            Ok(())
        }
        Err(err) => {
            eprintln!("{err}");
            eprintln!("Usage: aorta-tui [--group <GROUP>] [--no-client-override]");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_args, CommandLine};

    #[test]
    fn parse_group_and_client_override_flag() {
        let parsed = parse_args([
            "--group".to_string(),
            "demo".to_string(),
            "--no-client-override".to_string(),
        ]);
        assert_eq!(
            parsed,
            Ok(CommandLine::Run {
                group: Some("demo".to_string()),
                no_client_override: true,
            })
        );
    }

    #[test]
    fn reject_group_when_next_token_is_another_flag() {
        let parsed = parse_args(["--group".to_string(), "--no-client-override".to_string()]);
        assert!(
            parsed.is_err(),
            "expected a usage error for a missing group value"
        );
    }

    #[test]
    fn reject_group_when_value_is_missing() {
        let parsed = parse_args(["--group".to_string()]);
        assert!(
            parsed.is_err(),
            "expected a usage error for a missing group value"
        );
    }
}
