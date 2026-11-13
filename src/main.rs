use quorum::{
    Result, paxos_account,
    projection::{Projection, Tape},
    replay::{self, Mode},
    scenarios, vote,
};
use serde_json::json;
use std::{env, fs, io};

fn run() -> Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.first().map(String::as_str).unwrap_or("raft") {
        "raft" | "ping-pong" | "calm" | "duel-inclusion" | "duel-exclusion" => {
            scenarios::capture(
                args.first().map(String::as_str).unwrap_or("raft"),
                Box::new(io::stdout()),
            )?;
        }
        "capture" => {
            if args.len() != 3 {
                return Err(
                    "usage: quorum capture <raft|ping-pong|calm|duel-inclusion|duel-exclusion> <tape.jsonl>"
                        .into(),
                );
            }
            // Validate the requested scenario before opening its output path.
            scenarios::header(&args[1])?;
            let output = fs::File::create(&args[2]).map_err(|e| e.to_string())?;
            let sim = scenarios::capture(&args[1], Box::new(output))?;
            println!("{}", sim.check_projection()?.summary());
        }
        "paxos-account" => {
            if !(args.len() == 2 || (args.len() == 3 && args[2] == "--json")) {
                return Err("usage: quorum paxos-account <tape.jsonl> [--json]".into());
            }
            let bytes = fs::read(&args[1]).map_err(|e| e.to_string())?;
            let report = paxos_account::examine(&bytes);
            if args.len() == 3 {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?
                );
            } else {
                print!("{}", paxos_account::human(&report));
            }
        }
        "inspect" | "replay" | "diagnose" | "vote" | "prefixes" => {
            if args.len() != 2 {
                return Err(
                    "usage: quorum <inspect|replay|diagnose|vote|prefixes> <tape.jsonl>".into(),
                );
            }
            let bytes = fs::read(&args[1]).map_err(|e| e.to_string())?;
            let tape = Tape::read(&bytes)?;
            let report = match args[0].as_str() {
                "inspect" => {
                    json!({"source_sha256":tape.sha256,"projection":tape.projection.summary()})
                }
                "replay" | "diagnose" => serde_json::to_value(replay::replay(
                    &tape,
                    if args[0] == "replay" {
                        Mode::Strict
                    } else {
                        Mode::Diagnostic
                    },
                    scenarios::factory,
                )?)
                .unwrap(),
                "vote" => serde_json::to_value(vote::verify(&tape)?).unwrap(),
                "prefixes" => {
                    let mut classes = std::collections::BTreeMap::new();
                    for end in 0..=tape.rows.len() {
                        let p = Projection::from_rows(&tape.rows[..end])?;
                        *classes.entry(p.tail()).or_insert(0usize) += 1;
                    }
                    json!({"source_sha256":tape.sha256,"prefixes":tape.rows.len()+1,"tail_classes":classes})
                }
                _ => unreachable!(),
            };
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
        }
        _ => {
            return Err(
                "usage: quorum [raft|ping-pong|calm|duel-inclusion|duel-exclusion|capture|inspect|replay|diagnose|vote|prefixes|paxos-account]"
                    .into(),
            );
        }
    }
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
