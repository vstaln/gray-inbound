//! Ported wholesale from gray-send-message. Entry points renamed: manifest/handle/main → *_send_message.
//! gray-send-message — drop outside messages into the next agent turn.
//!
//! - `/tell <text>` appends `{"ts","text"}` to `~/.gray/send-message/queue.jsonl`
//!   and confirms via `host/say "queued: <text>"` (capability `host.say`; a
//!   failed say never loses the queued message).
//! - `agent/before_start` drains `queue.jsonl` and the plain-text drop file
//!   `~/.gray/send-message/inbox` (anything can `echo "hi" >> inbox`), then
//!   returns `{text}` — one user message, each line prefixed
//!   `[queued message]`. Empty queues → `{}`.
//!
//! Draining renames the file first, so a writer mid-drain lands in a fresh
//! queue for the following turn rather than being lost.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use serde_json::{Value, json};

/// How long a `/tell` waits on `host/say` before giving up (the message is
/// already queued at that point — the say is only confirmation).
const HOST_TTL: Duration = Duration::from_secs(10);

pub type Pending = Arc<Mutex<HashMap<String, mpsc::Sender<Value>>>>;

pub fn manifest_send_message() -> Value {
    json!({
        "name": "send-message",
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": "2.0",
        "tools": [],
        "commands": ["/tell"],
        "hooks": ["agent/before_start"],
        "capabilities": ["host.say"],
    })
}

fn state_dir() -> PathBuf {
    let home = std::env::var_os("GRAY_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".gray")))
        .unwrap_or_else(|| PathBuf::from("."));
    home.join("send-message")
}

fn queue_path() -> PathBuf {
    state_dir().join("queue.jsonl")
}

fn inbox_path() -> PathBuf {
    state_dir().join("inbox")
}

fn now_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Append one message to queue.jsonl.
fn enqueue(text: &str) -> std::io::Result<()> {
    let dir = state_dir();
    std::fs::create_dir_all(&dir)?;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(queue_path())?;
    writeln!(f, "{}", json!({ "ts": now_epoch(), "text": text }))
}

/// A queued line is JSON `{"text": …}` (queue.jsonl) or raw text (inbox).
/// Returns `None` for blank/unusable lines.
fn line_text(line: &str) -> Option<String> {
    let t = line.trim();
    if t.is_empty() {
        return None;
    }
    if let Ok(v) = serde_json::from_str::<Value>(t)
        && let Some(s) = v.get("text").and_then(Value::as_str)
    {
        let s = s.trim();
        return if s.is_empty() { None } else { Some(s.to_string()) };
    }
    Some(t.to_string())
}

/// Rename-then-read `path`: returns pending lines, leaves the file gone.
fn drain(path: &PathBuf) -> Vec<String> {
    if !path.exists() {
        return vec![];
    }
    let staging = path.with_extension("draining");
    if std::fs::rename(path, &staging).is_err() {
        return vec![];
    }
    let out = std::fs::read_to_string(&staging)
        .map(|s| s.lines().filter_map(line_text).collect())
        .unwrap_or_default();
    let _ = std::fs::remove_file(&staging);
    out
}

/// `agent/before_start`: all pending messages → one prefixed user message.
fn on_before_start() -> Value {
    let mut msgs = drain(&queue_path());
    msgs.extend(drain(&inbox_path()));
    if msgs.is_empty() {
        return json!({});
    }
    let text = msgs
        .iter()
        .map(|m| format!("[queued message] {m}"))
        .collect::<Vec<_>>()
        .join("\n");
    json!({ "text": text })
}

/// `/tell <text>` — `argv` excludes the command name.
fn run_command(argv: &[&str], say: &mut dyn FnMut(&str) -> Result<(), String>) -> String {
    let text = argv.join(" ");
    if text.trim().is_empty() {
        return "Usage: /tell <message> — queued into the next agent turn".into();
    }
    let text = text.trim();
    match enqueue(text) {
        Ok(()) => {
            let _ = say(&format!("queued: {text}"));
            format!("queued for next turn: {text}")
        }
        Err(e) => format!("couldn't queue message: {e}"),
    }
}

/// One request → `Some(reply)`, or `None` for notifications. The bool asks
/// the loop to exit after writing the reply.
pub fn handle_send_message(
    req: &Value,
    say: &mut dyn FnMut(&str) -> Result<(), String>,
) -> (Option<Value>, bool) {
    let id = req.get("id").cloned();
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(Value::Null);
    let Some(id) = id else {
        return (None, method == "plugin/shutdown");
    };
    let result = match method {
        "plugin/manifest" => manifest_send_message(),
        "agent/before_start" => on_before_start(),
        "command/run" => {
            let argv: Vec<&str> = params
                .get("argv")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            json!({ "text": run_command(&argv, say) })
        }
        "plugin/shutdown" => return (Some(json!({ "id": id, "result": {} })), true),
        _ => {
            let error = json!({ "code": -32601, "message": "method not found" });
            return (Some(json!({ "id": id, "error": error })), false);
        }
    };
    (Some(json!({ "id": id, "result": result })), false)
}

/// sidecar→host request: string id, reply routed by the reader thread.
pub fn host_call(
    out: &Arc<Mutex<std::io::Stdout>>,
    pending: &Pending,
    counter: &Arc<Mutex<u64>>,
    method: &str,
    params: Value,
) -> Result<Value, String> {
    let id = {
        let mut n = counter.lock().expect("id counter");
        *n += 1;
        format!("q{n}")
    };
    let (tx, rx) = mpsc::channel();
    pending.lock().expect("pending").insert(id.clone(), tx);
    {
        let req = json!({ "id": id, "method": method, "params": params });
        let mut o = out.lock().expect("stdout");
        if writeln!(o, "{req}").and_then(|_| o.flush()).is_err() {
            pending.lock().expect("pending").remove(&id);
            return Err("stdout closed".into());
        }
    }
    let v = match rx.recv_timeout(HOST_TTL) {
        Ok(v) => v,
        Err(_) => {
            pending.lock().expect("pending").remove(&id);
            return Err("host timeout".into());
        }
    };
    match v.get("error") {
        Some(e) => Err(e
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("host error")
            .to_string()),
        None => Ok(v.get("result").cloned().unwrap_or(Value::Null)),
    }
}

fn main_send_message() -> std::io::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("manifest") {
        println!("{}", manifest_send_message());
        return Ok(());
    }
    let stdout = Arc::new(Mutex::new(std::io::stdout()));
    let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
    let counter = Arc::new(Mutex::new(0u64));

    // Reader thread: `host/*` replies (string id, no method) route by id into
    // `pending`; everything else queues for the work loop.
    let (work_tx, work_rx) = mpsc::channel::<Value>();
    let reader_pending = pending.clone();
    let _reader = std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
            if v.get("method").is_none() {
                if let Some(id) = v.get("id").and_then(Value::as_str)
                    && let Some(tx) = reader_pending.lock().expect("pending").remove(id)
                {
                    let _ = tx.send(v);
                }
                continue;
            }
            if work_tx.send(v).is_err() {
                break;
            }
        }
    });

    for req in work_rx {
        let mut say = |t: &str| {
            host_call(&stdout, &pending, &counter, "host/say", json!({ "text": t }))
                .map(|_| ())
        };
        let (reply, exit) = handle_send_message(&req, &mut say);
        if let Some(reply) = reply {
            let mut o = stdout.lock().expect("stdout");
            let _ = writeln!(o, "{reply}");
            let _ = o.flush();
        }
        if exit {
            break;
        }
    }
    // The reader thread is a detached stdin pump; returning from main
    // ends the process with it, so there is nothing to join.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// GRAY_HOME is process-global: env-touching tests hold this lock.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn ok_say(said: &mut Vec<String>) -> impl FnMut(&str) -> Result<(), String> + '_ {
        |t| {
            said.push(t.to_string());
            Ok(())
        }
    }

    #[test]
    fn manifest_claims_before_start_at_2_0() {
        let m = manifest_send_message();
        assert_eq!(m["name"], "send-message");
        assert_eq!(m["protocol"], "2.0");
        assert_eq!(m["hooks"], json!(["agent/before_start"]));
        assert_eq!(m["commands"], json!(["/tell"]));
        assert_eq!(m["capabilities"], json!(["host.say"]));
    }

    #[test]
    fn line_text_accepts_json_and_raw() {
        assert_eq!(line_text(r#"{"ts":1,"text":"hi there"}"#).as_deref(), Some("hi there"));
        assert_eq!(line_text("  raw line ").as_deref(), Some("raw line"));
        assert_eq!(line_text(""), None);
        assert_eq!(line_text(r#"{"text":"  "}"#), None);
    }

    #[test]
    fn drain_returns_lines_and_removes_file() {
        let dir = std::env::temp_dir().join(format!("sm-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let q = dir.join("queue.jsonl");
        std::fs::write(&q, "{\"text\":\"one\"}\ntwo\n\n").unwrap();
        assert_eq!(drain(&q), vec!["one".to_string(), "two".to_string()]);
        assert!(!q.exists());
        assert_eq!(drain(&q), Vec::<String>::new());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tell_enqueues_and_confirms() {
        let _g = ENV_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join(format!("sm-tell-{}", std::process::id()));
        // Redirect state dir via env for this test.
        unsafe { std::env::set_var("GRAY_HOME", &dir) };
        let mut said = vec![];
        let out = {
            let mut f = ok_say(&mut said);
            run_command(&["hello", "world"], &mut f)
        };
        assert!(out.contains("queued for next turn: hello world"));
        assert_eq!(said, vec!["queued: hello world".to_string()]);
        let r = on_before_start();
        assert_eq!(r["text"], "[queued message] hello world");
        // drained: second call yields {}
        assert_eq!(on_before_start(), json!({}));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn inbox_lines_are_delivered() {
        let _g = ENV_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join(format!("sm-inbox-{}", std::process::id()));
        unsafe { std::env::set_var("GRAY_HOME", &dir) };
        let smdir = dir.join("send-message");
        std::fs::create_dir_all(&smdir).unwrap();
        std::fs::write(smdir.join("inbox"), "from cron job\n").unwrap();
        let r = on_before_start();
        assert_eq!(r["text"], "[queued message] from cron job");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_queue_yields_empty_result() {
        let _g = ENV_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join(format!("sm-empty-{}", std::process::id()));
        unsafe { std::env::set_var("GRAY_HOME", &dir) };
        assert_eq!(on_before_start(), json!({}));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tell_without_text_is_usage() {
        let mut said = vec![];
        let out = {
            let mut f = ok_say(&mut said);
            run_command(&[], &mut f)
        };
        assert!(out.contains("Usage: /tell"));
        assert!(said.is_empty());
    }

    #[test]
    fn shutdown_replies_then_exits() {
        let mut f = |_: &str| Ok(());
        let (reply, exit) = handle_send_message(&json!({ "id": 2, "method": "plugin/shutdown" }), &mut f);
        assert!(reply.is_some() && exit);
    }
}
