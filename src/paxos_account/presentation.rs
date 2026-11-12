use serde_json::Value;
use std::fmt::Write;

/// Render the independent account without replacing its four peer sections
/// with an overall verdict. All evidence details remain visible beneath them.
pub fn human(report: &Value) -> String {
    let mut text = String::new();
    writeln!(text, "Independent Multi-Paxos account").unwrap();
    writeln!(text, "Evidence references are one-based JSONL lines.\n").unwrap();
    for (key, label) in [
        ("schema_version", "Schema version"),
        ("account_version", "Account version"),
        ("interpretation", "Interpretation"),
        ("source_sha256", "Source SHA-256"),
    ] {
        field(&mut text, label, report.get(key), 0);
    }
    section(&mut text, "World", report.get("world"));
    section(&mut text, "Coverage", report.get("coverage"));
    section(
        &mut text,
        "Interpretation findings",
        report.get("interpretation_findings"),
    );
    peers(&mut text, "Choices", report.get("choices"), "choice");
    peers(
        &mut text,
        "Obligations",
        report.get("obligations"),
        "obligation",
    );
    peers(&mut text, "Claims", report.get("claims"), "claim");
    peers(&mut text, "Attempts", report.get("attempts"), "attempt");
    section(&mut text, "Observations", report.get("observations"));
    section(
        &mut text,
        "Interpretation gap dependencies",
        report.get("interpretation_gap_dependencies"),
    );
    text
}

fn indent(text: &mut String, depth: usize) {
    for _ in 0..depth {
        text.push_str("  ");
    }
}

fn scalar(text: &mut String, value: &Value) {
    // JSON quoting preserves arbitrary client values and makes embedded newlines
    // unambiguous; it never gives a protocol meaning to a value's object keys.
    write!(text, "{value}").unwrap();
}

fn inline(value: &Value) -> bool {
    match value {
        Value::Object(object) => object.is_empty(),
        Value::Array(array) => array
            .iter()
            .all(|item| !item.is_object() && !item.is_array()),
        _ => true,
    }
}

fn field(text: &mut String, label: &str, value: Option<&Value>, depth: usize) {
    indent(text, depth);
    write!(text, "{label}:").unwrap();
    match value {
        None => text.push_str(" not recorded\n"),
        Some(value) if matches!(label, "value" | "entry" | "failed_shape") || inline(value) => {
            text.push(' ');
            scalar(text, value);
            text.push('\n');
        }
        Some(value) => {
            if let Some(verdict) = value.get("verdict").and_then(Value::as_str) {
                write!(text, " [{verdict}]").unwrap();
                if let Some(line) = value.get("boundary_line") {
                    write!(text, " at line {line}").unwrap();
                }
            }
            text.push('\n');
            details(text, value, depth + 1);
        }
    }
}

fn details(text: &mut String, value: &Value, depth: usize) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                field(text, key, Some(value), depth);
            }
        }
        Value::Array(items) => {
            if items.is_empty() {
                indent(text, depth);
                text.push_str("(none recorded)\n");
            }
            for (index, item) in items.iter().enumerate() {
                indent(text, depth);
                write!(text, "- {}", index + 1).unwrap();
                // Each nested path/candidate remains separate, including failed
                // alternatives beside a successful proof path.
                if inline(item) {
                    text.push_str(": ");
                    scalar(text, item);
                    text.push('\n');
                } else {
                    text.push_str(":\n");
                    details(text, item, depth + 1);
                }
            }
        }
        _ => {
            indent(text, depth);
            scalar(text, value);
            text.push('\n');
        }
    }
}

fn section(text: &mut String, title: &str, value: Option<&Value>) {
    write!(text, "\n{title}\n").unwrap();
    match value {
        Some(value) => details(text, value, 1),
        None => text.push_str("  not recorded\n"),
    }
}

fn peers(text: &mut String, title: &str, value: Option<&Value>, kind: &str) {
    let Some(items) = value.and_then(Value::as_array) else {
        write!(text, "\n{title}\n  not recorded\n").unwrap();
        return;
    };
    write!(text, "\n{title} ({})\n", items.len()).unwrap();
    if items.is_empty() {
        text.push_str("  (none recorded; see coverage)\n");
    }
    for (index, item) in items.iter().enumerate() {
        write!(text, "  {}. {kind}", index + 1).unwrap();
        if let Some(id) = item.get("id").and_then(Value::as_str) {
            write!(text, " {id}").unwrap();
        }
        if let Some(verdict) = item.get("verdict").and_then(Value::as_str) {
            write!(text, " [{verdict}]").unwrap();
        }
        for key in [
            "proposition",
            "kind",
            "node",
            "proposer",
            "slot",
            "ballot",
            "value",
        ] {
            if let Some(value) = item.get(key) {
                write!(text, "; {key}=").unwrap();
                scalar(text, value);
            }
        }
        if let Some(line) = item.get("boundary_line") {
            write!(text, "; boundary line {line}").unwrap();
        }
        if let Some(line) = item.get("first_quorum_line") {
            write!(text, "; first quorum line {line}").unwrap();
        }
        text.push('\n');
        details(text, item, 2);
    }
}
