//! gray-inbound — one sidecar for every "outside event → agent turn" plugin.
//!
//! Merges gray-send-message, gray-webhooks, gray-file-trigger and
//! gray-control. Each source lives verbatim in `mods/` with its own state
//! paths (`~/.gray/send-message/`, `~/.gray/webhooks/`,
//! `~/.gray/file-trigger/`, `~/.gray/control/`) and slash commands — only the
//! wire plumbing is shared.
//!
//! Shared plumbing: ONE stdout, ONE `q{n}` id counter and ONE pending-reply
//! map for sidecar→host calls, so `host/say` (send-message, control) and
//! `host/run` (webhooks) ids never collide. The stdin reader thread routes
//! host replies by id; everything else feeds the work loop.
//!
//! Listener threads (all preserved):
//! - stdin reader (this file)
//! - webhooks HTTP accept loop on 127.0.0.1 (started by `agent/before_start`
//!   when routes exist, or by `/webhook add`)
//! - control unix-socket accept loop at `~/.gray/control/<sid>.sock`
//!   (started on the first request that names a session)
//!
//! Hook fan-out:
//! - `agent/before_start` → send-message + webhooks + control: `{text}`
//!   results joined with blank lines (each module contributes or abstains).
//! - `tool/before`, `tool/after` → file-trigger.
//! - `tool/call`, `command/run` → routed by `params.name` to the owner.

use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, mpsc};

use serde_json::{Value, json};

mod parts;

#[allow(dead_code)]
#[path = "mods/control.rs"]
mod control;
#[allow(dead_code)]
#[path = "mods/file_trigger.rs"]
mod file_trigger;
#[allow(dead_code)]
#[path = "mods/send_message.rs"]
mod send_message;
#[allow(dead_code)]
#[path = "mods/webhooks.rs"]
mod webhooks;

const META_COMMAND: &str = "/inbound";

const PARTS: &[parts::Part] = &[
    parts::Part {
        name: "send-message",
        default: true,
        description: "queue /tell messages for the next turn",
    },
    parts::Part {
        name: "webhooks",
        default: true,
        description: "local HTTP webhooks that start turns",
    },
    parts::Part {
        name: "file-trigger",
        default: true,
        description: "per-path reminders appended to write results",
    },
    parts::Part {
        name: "control",
        default: true,
        description: "unix-socket remote control and send_to_session",
    },
];

/// Union of the constituent manifests (tools dedup by name — no collisions
/// today), plus the `/inbound` meta command.
fn manifest(parts: &parts::Parts) -> Value {
    let mut sources = Vec::new();
    if parts.on("send-message") {
        sources.push(send_message::manifest_send_message());
    }
    if parts.on("webhooks") {
        sources.push(webhooks::manifest_webhooks());
    }
    if parts.on("file-trigger") {
        sources.push(file_trigger::manifest_file_trigger());
    }
    if parts.on("control") {
        sources.push(control::manifest_control());
    }
    let mut tools: Vec<Value> = Vec::new();
    let mut commands: Vec<Value> = Vec::new();
    let mut hooks: Vec<Value> = Vec::new();
    let mut caps: Vec<Value> = Vec::new();
    let union = |dst: &mut Vec<Value>, src: Option<&Value>| {
        for v in src.and_then(Value::as_array).into_iter().flatten() {
            if !dst.contains(v) {
                dst.push(v.clone());
            }
        }
    };
    for m in &sources {
        for t in m
            .get("tools")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if !tools.iter().any(|x| x.get("name") == t.get("name")) {
                tools.push(t.clone());
            }
        }
        union(&mut commands, m.get("commands"));
        union(&mut hooks, m.get("hooks"));
        union(&mut caps, m.get("capabilities"));
    }
    commands.push(json!(META_COMMAND));
    let mut descriptions = serde_json::Map::new();
    descriptions.insert(
        META_COMMAND.into(),
        json!("list the inbound message sources"),
    );
    if parts.on("send-message") {
        descriptions.insert("/tell".into(), json!("queue a message for the next turn"));
    }
    if parts.on("webhooks") {
        descriptions.insert("/webhook".into(), json!("add and inspect inbound webhooks"));
    }
    if parts.on("file-trigger") {
        descriptions.insert(
            "/trigger".into(),
            json!("list or reload per-path reminder rules"),
        );
    }
    if parts.on("control") {
        descriptions.insert(
            "/control".into(),
            json!("show and manage the remote-control socket"),
        );
    }
    json!({
        "name": "inbound",
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": "2.0",
        "capabilities": caps,
        "tools": tools,
        "commands": commands,
        "command_descriptions": descriptions,
        "hooks": hooks,
        "parts": parts.manifest(PARTS),
    })
}

/// Module state held for the life of the process.
struct Inbound {
    wh: Arc<webhooks::Shared>,
    ctl: Arc<control::Shared>,
    ft: file_trigger::State,
}

impl Inbound {
    fn new() -> Self {
        let wh = webhooks::Shared::new();
        let mut ctl = control::Shared::new();
        // One stdout + one id counter across every module.
        ctl.out = wh.out.clone();
        ctl.counter = wh.counter.clone();
        let ft = file_trigger::State {
            stash: Mutex::new(HashMap::new()),
            rules: Mutex::new(file_trigger::load_rules()),
        };
        Self {
            wh: Arc::new(wh),
            ctl: Arc::new(ctl),
            ft,
        }
    }

    fn send_message(&self, req: &Value) -> (Option<Value>, bool) {
        let wh = &self.wh;
        let mut say = |t: &str| {
            send_message::host_call(
                &wh.out,
                &wh.pending,
                &wh.counter,
                "host/say",
                json!({ "text": t }),
            )
            .map(|_| ())
        };
        send_message::handle_send_message(req, &mut say)
    }

    /// control binds its socket lazily on the first hook naming a session;
    /// in the merged plugin that can be any module's hook.
    fn touch_session(&self, params: &Value) {
        if params.get("session").is_some() {
            control::note_session(&self.ctl, params);
            control::ensure_listener(&self.ctl);
        }
    }

    fn stop_all(&self) {
        self.wh.stop.store(true, Ordering::SeqCst);
        self.ctl.stop.store(true, Ordering::SeqCst);
        if let Some(sock) = self.ctl.session.lock().expect("session").sock.take() {
            let _ = std::fs::remove_file(sock);
        }
    }
}

fn result_of(reply: Option<Value>) -> Value {
    reply
        .and_then(|r| r.get("result").cloned())
        .unwrap_or_else(|| json!({}))
}

/// `agent/before_start` fan-out: join every module's non-empty `{text}`.
fn before_start(st: &Inbound, req: &Value, parts: &parts::Parts) -> Value {
    let mut replies = Vec::new();
    if parts.on("send-message") {
        replies.push(st.send_message(req).0);
    }
    if parts.on("webhooks") {
        replies.push(webhooks::handle_webhooks(&st.wh, req).0);
    }
    if parts.on("control") {
        replies.push(control::handle_control(&st.ctl, req).0);
    }
    let parts: Vec<String> = replies
        .into_iter()
        .filter_map(|r| {
            let t = r?
                .get("result")?
                .get("text")?
                .as_str()?
                .trim_end()
                .to_string();
            (!t.is_empty()).then_some(t)
        })
        .collect();
    if parts.is_empty() {
        json!({})
    } else {
        json!({ "text": parts.join("\n\n") })
    }
}

/// `/inbound` — merged-plugin overview with live listener state.
fn inbound_command(st: &Inbound, parts: &parts::Parts) -> String {
    let sock = st
        .ctl
        .session
        .lock()
        .expect("session")
        .sock
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "not bound yet".into());
    let mut lines: Vec<String> = vec![format!(
        "gray-inbound {} — one sidecar, four inbound sources:",
        env!("CARGO_PKG_VERSION")
    )];
    if parts.on("send-message") {
        lines.push(
            "\x20 /tell      queue a message for the next turn (~/.gray/send-message/)".into(),
        );
    }
    if parts.on("webhooks") {
        lines.push(format!(
            "\x20 /webhook   HTTP webhooks → turns ({})",
            webhooks::listener_state(&st.wh)
        ));
    }
    if parts.on("file-trigger") {
        lines.push(
            "\x20 /trigger   per-path reminders appended to write results (~/.gray/file-trigger/rules.json)"
                .into(),
        );
    }
    if parts.on("control") {
        lines.push(format!(
            "\x20 /control   remote control socket (sock: {sock})"
        ));
    }
    let mut tools: Vec<&str> = Vec::new();
    if parts.on("webhooks") {
        tools.push("webhook_emit");
    }
    if parts.on("control") {
        tools.push("send_to_session");
    }
    if !tools.is_empty() {
        lines.push(format!("\x20 tools: {}", tools.join(" · ")));
    }
    lines.join("\n")
}

fn handle(st: &Inbound, req: &Value, parts: &parts::Parts) -> (Option<Value>, bool) {
    let id = req.get("id").cloned();
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(Value::Null);
    let Some(id) = id else {
        if method == "plugin/shutdown" {
            st.stop_all();
            return (None, true);
        }
        return (None, false);
    };
    if method != "plugin/shutdown" {
        st.touch_session(&params);
    }
    let result = match method {
        "plugin/manifest" => manifest(parts),
        "agent/before_start" => before_start(st, req, parts),
        "tool/before" | "tool/after" => {
            if !parts.on("file-trigger") {
                return (Some(json!({ "id": id, "result": {} })), false);
            }
            let (reply, _) = file_trigger::handle_file_trigger(&st.ft, req);
            return (reply, false);
        }
        "tool/call" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            let (reply, _) = match name {
                "webhook_emit" if parts.on("webhooks") => webhooks::handle_webhooks(&st.wh, req),
                "send_to_session" if parts.on("control") => control::handle_control(&st.ctl, req),
                other => {
                    let r =
                        json!({ "content": format!("unknown tool: {other}"), "is_error": true });
                    return (Some(json!({ "id": id, "result": r })), false);
                }
            };
            return (reply, false);
        }
        "command/run" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            match name {
                META_COMMAND => json!({ "text": inbound_command(st, parts) }),
                "/tell" if parts.on("send-message") => result_of(st.send_message(req).0),
                "/webhook" if parts.on("webhooks") => {
                    result_of(webhooks::handle_webhooks(&st.wh, req).0)
                }
                "/trigger" if parts.on("file-trigger") => {
                    result_of(file_trigger::handle_file_trigger(&st.ft, req).0)
                }
                "/control" if parts.on("control") => {
                    result_of(control::handle_control(&st.ctl, req).0)
                }
                _ => json!({ "text": format!("unknown command {name} — try {META_COMMAND}") }),
            }
        }
        "plugin/shutdown" => {
            st.stop_all();
            return (Some(json!({ "id": id, "result": {} })), true);
        }
        _ => {
            let error = json!({ "code": -32601, "message": "method not found" });
            return (Some(json!({ "id": id, "error": error })), false);
        }
    };
    (Some(json!({ "id": id, "result": result })), false)
}

fn main() {
    let parts = parts::Parts::load("inbound", PARTS);
    if std::env::args().nth(1).as_deref() == Some("manifest") {
        println!("{}", manifest(&parts));
        return;
    }
    let st = Inbound::new();
    let pending = st.wh.pending.clone();
    let out = st.wh.out.clone();

    // Reader thread: host→sidecar replies (string id, no method) route to
    // pending waiters (or are dropped — control's host/say is fire-and-forget);
    // requests and notifications feed the work loop.
    let (work_tx, work_rx) = mpsc::channel::<Value>();
    let _reader = std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            let Ok(v) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if v.get("method").is_none()
                && let Some(id) = v.get("id").and_then(Value::as_str)
            {
                if let Some(tx) = pending.lock().expect("pending").remove(id) {
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
        let (reply, exit) = handle(&st, &req, &parts);
        if let Some(reply) = reply {
            let mut o = out.lock().expect("stdout");
            let _ = writeln!(o, "{reply}");
            let _ = o.flush();
        }
        if exit {
            break;
        }
    }
    st.stop_all();
}
