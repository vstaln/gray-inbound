//! Ported wholesale from gray-file-trigger. Entry points renamed: manifest/handle/main → *_file_trigger.
//! gray-file-trigger — per-path reminders appended to write results.
//!
//! `~/.gray/file-trigger/rules.json` holds
//! `[{"glob": "**/*.rs", "message": "ran cargo fmt check yourself"}, …]`.
//! When a write/edit tool's path arg matches a glob, the rule's message is
//! appended to the tool result (`tool/after` carries no args, so
//! `tool/before` stashes the path per (session, tool) in memory).
//!
//! `/trigger list` shows rules · `/trigger reload` re-reads rules.json.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use serde_json::{Value, json};

/// Write-shaped tools whose path arg triggers rules.
const WRITE_TOOLS: &[&str] = &[
    "write", "edit", "patch", "write_file", "apply_patch", "multi_edit", "notebook_edit",
];

#[derive(Debug, Clone)]
pub struct Rule {
    glob: String,
    pattern: Option<glob::Pattern>,
    message: String,
}

/// The user's home dir: `$HOME`, or `%USERPROFILE%` on Windows where
/// HOME is typically unset for a natively-spawned process.
fn user_home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

fn gray_home() -> PathBuf {
    std::env::var_os("GRAY_HOME")
        .map(PathBuf::from)
        .or_else(|| user_home().map(|h| h.join(".gray")))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn rules_path() -> PathBuf {
    gray_home().join("file-trigger").join("rules.json")
}

/// Read rules.json. Bad entries (missing fields, bad glob) are skipped.
pub fn load_rules() -> Vec<Rule> {
    let Ok(raw) = std::fs::read_to_string(rules_path()) else {
        return Vec::new();
    };
    let Ok(v) = serde_json::from_str::<Value>(&raw) else {
        return Vec::new();
    };
    let Some(list) = v.as_array() else {
        return Vec::new();
    };
    list.iter()
        .filter_map(|r| {
            let glob = r.get("glob").and_then(Value::as_str)?.to_string();
            let message = r.get("message").and_then(Value::as_str)?.to_string();
            Some(Rule {
                pattern: glob::Pattern::new(&glob).ok(),
                glob,
                message,
            })
        })
        .collect()
}

/// (session, tool) → path, stashed by tool/before for tool/after.
pub type Stash = Mutex<HashMap<(String, String), String>>;
pub type Rules = Mutex<Vec<Rule>>;

pub struct State {
    pub stash: Stash,
    pub rules: Rules,
}

fn session_key(params: &Value) -> (String, String) {
    let sid = params
        .get("session")
        .and_then(|s| s.get("id"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let name = params.get("name").and_then(Value::as_str).unwrap_or("").to_string();
    (sid, name)
}

fn extract_path(args: &Value) -> Option<String> {
    for key in ["path", "file_path"] {
        if let Some(p) = args.get(key).and_then(Value::as_str) {
            return Some(p.to_string());
        }
    }
    args.as_object().and_then(|m| {
        m.iter()
            .find(|(k, v)| k.contains("path") && v.is_string())
            .and_then(|(_, v)| v.as_str().map(str::to_string))
    })
}

fn tool_before(state: &State, params: &Value) -> Value {
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    if !WRITE_TOOLS.contains(&name) {
        return json!({"decision": "allow"});
    }
    let args = params.get("args").cloned().unwrap_or(json!({}));
    if let Some(path) = extract_path(&args) {
        state
            .stash
            .lock()
            .expect("stash")
            .insert(session_key(params), path);
    }
    json!({"decision": "allow"})
}

fn tool_after(state: &State, params: &Value) -> Value {
    let key = session_key(params);
    let Some(path) = state.stash.lock().expect("stash").remove(&key) else {
        return json!({});
    };
    let messages: Vec<String> = state
        .rules
        .lock()
        .expect("rules")
        .iter()
        .filter(|r| r.pattern.as_ref().is_some_and(|p| p.matches(&path)))
        .map(|r| r.message.clone())
        .collect();
    if messages.is_empty() {
        return json!({});
    }
    let content = params.get("content").and_then(Value::as_str).unwrap_or("");
    json!({ "content": format!("{content}\n\n{}", messages.join("\n")) })
}

/// `/trigger …` — `argv` excludes the command name.
fn run_command(state: &State, argv: &[&str]) -> String {
    match argv.first().copied() {
        Some("reload") => {
            let fresh = load_rules();
            let n = fresh.len();
            *state.rules.lock().expect("rules") = fresh;
            format!("reloaded {} rule{} from {}", n, if n == 1 { "" } else { "s" }, rules_path().display())
        }
        Some("list") | None => {
            let rules = state.rules.lock().expect("rules");
            if rules.is_empty() {
                format!("no rules — add them to {}", rules_path().display())
            } else {
                let mut lines = vec![format!("{} rule{}:", rules.len(), if rules.len() == 1 { "" } else { "s" })];
                for r in rules.iter() {
                    let ok = if r.pattern.is_some() { "" } else { " (invalid glob)" };
                    lines.push(format!("  {}{} → {}", r.glob, ok, r.message));
                }
                lines.join("\n")
            }
        }
        _ => "usage: /trigger list | /trigger reload".into(),
    }
}

pub fn manifest_file_trigger() -> Value {
    json!({
        "name": "file-trigger",
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": "2.0",
        "tools": [],
        "commands": ["/trigger"],
        "hooks": ["tool/before", "tool/after"],
    })
}

/// One request → `Some(reply)`, or `None` for notifications. The bool asks
/// the loop to exit after writing the reply.
pub fn handle_file_trigger(state: &State, req: &Value) -> (Option<Value>, bool) {
    let id = req.get("id").cloned();
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(Value::Null);
    let Some(id) = id else {
        return (None, method == "plugin/shutdown");
    };
    let result = match method {
        "plugin/manifest" => manifest_file_trigger(),
        "tool/before" => tool_before(state, &params),
        "tool/after" => tool_after(state, &params),
        "command/run" => {
            let argv: Vec<&str> = params
                .get("argv")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            json!({ "text": run_command(state, &argv) })
        }
        "plugin/shutdown" => return (Some(json!({ "id": id, "result": {} })), true),
        _ => {
            let error = json!({ "code": -32601, "message": "method not found" });
            return (Some(json!({ "id": id, "error": error })), false);
        }
    };
    (Some(json!({ "id": id, "result": result })), false)
}

fn main_file_trigger() -> std::io::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("manifest") {
        println!("{}", manifest_file_trigger());
        return Ok(());
    }
    let state = State {
        stash: Mutex::new(HashMap::new()),
        rules: Mutex::new(load_rules()),
    };
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        let Ok(req) = serde_json::from_str::<Value>(&line) else { continue };
        let (reply, exit) = handle_file_trigger(&state, &req);
        if let Some(reply) = reply {
            writeln!(stdout, "{reply}")?;
            stdout.flush()?;
        }
        if exit {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with(rules: Vec<Rule>) -> State {
        State {
            stash: Mutex::new(HashMap::new()),
            rules: Mutex::new(rules),
        }
    }

    fn rule(glob: &str, message: &str) -> Rule {
        Rule {
            glob: glob.to_string(),
            pattern: glob::Pattern::new(glob).ok(),
            message: message.to_string(),
        }
    }

    fn call(state: &State, method: &str, params: Value) -> Value {
        handle_file_trigger(state, &json!({ "id": 1, "method": method, "params": params }))
            .0
            .unwrap()
    }

    #[test]
    fn manifest_shape() {
        let m = manifest_file_trigger();
        assert_eq!(m["name"], "file-trigger");
        assert_eq!(m["protocol"], "2.0");
        assert_eq!(m["hooks"], json!(["tool/before", "tool/after"]));
        assert_eq!(m["commands"], json!(["/trigger"]));
    }

    #[test]
    fn matching_glob_appends_message() {
        let s = state_with(vec![rule("**/*.rs", "ran cargo fmt check yourself")]);
        let before = call(
            &s,
            "tool/before",
            json!({"name": "write", "args": {"path": "src/main.rs", "content": "x"},
                   "session": {"id": "s1"}}),
        );
        assert_eq!(before["result"]["decision"], "allow");
        let after = call(
            &s,
            "tool/after",
            json!({"name": "write", "content": "done", "is_error": false, "session": {"id": "s1"}}),
        );
        assert_eq!(
            after["result"]["content"],
            "done\n\nran cargo fmt check yourself"
        );
    }

    #[test]
    fn nonmatching_glob_keeps_result() {
        let s = state_with(vec![rule("**/*.rs", "fmt")]);
        call(
            &s,
            "tool/before",
            json!({"name": "write", "args": {"path": "a.py", "content": "x"}, "session": {"id": "s2"}}),
        );
        let after = call(
            &s,
            "tool/after",
            json!({"name": "write", "content": "done", "is_error": false, "session": {"id": "s2"}}),
        );
        assert_eq!(after["result"], json!({}));
    }

    #[test]
    fn non_write_tools_dont_stash() {
        let s = state_with(vec![rule("**/*", "hi")]);
        call(
            &s,
            "tool/before",
            json!({"name": "read", "args": {"path": "a.rs"}, "session": {"id": "s3"}}),
        );
        let after = call(
            &s,
            "tool/after",
            json!({"name": "read", "content": "x", "is_error": false, "session": {"id": "s3"}}),
        );
        assert_eq!(after["result"], json!({}));
    }

    #[test]
    fn multiple_matches_join() {
        let s = state_with(vec![rule("**/*.rs", "fmt"), rule("src/**", "check clippy too")]);
        call(
            &s,
            "tool/before",
            json!({"name": "edit", "args": {"path": "src/lib.rs"}, "session": {"id": "s4"}}),
        );
        let after = call(
            &s,
            "tool/after",
            json!({"name": "edit", "content": "ok", "is_error": false, "session": {"id": "s4"}}),
        );
        let c = after["result"]["content"].as_str().unwrap_or("");
        assert!(c.contains("fmt") && c.contains("clippy"));
    }

    #[test]
    fn command_lists_rules() {
        let s = state_with(vec![rule("**/*.rs", "fmt")]);
        let r = call(&s, "command/run", json!({"name": "/trigger", "argv": ["list"]}));
        assert!(r["result"]["text"].as_str().unwrap().contains("**/*.rs"));
    }
}
