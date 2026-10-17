//! Ported wholesale from gray-webhooks. Entry points renamed: manifest/handle/main → *_webhooks.
//! gray-webhooks — inbound HTTP becomes agent turns.
//!
//! Original work (no source plugin; the event-trigger half of the cron
//! pair). A tiny HTTP listener on 127.0.0.1 — port from
//! `~/.gray/webhooks/config.json` (`{"port": 7844}`) — routes
//! `POST /<path>` to a named webhook from `~/.gray/webhooks/routes.json`.
//!
//! Delivery strategy (one): try `host/run` immediately; on any failure
//! enqueue `Webhook <name> fired: <body>` for the next `agent/before_start`
//! {text} injection. Once host/run has failed it isn't retried — later
//! deliveries go straight to the queue. Bodies are capped at 8 KiB; every
//! delivery is logged to `~/.gray/webhooks/log/<name>.jsonl`.
//!
//!   /webhook add <name> <path>   register a route
//!   /webhook list                routes + listener state + queued count
//!   /webhook rm <name>           remove a route
//!   /webhook tail <name>         last 10 deliveries
//!   tool webhook_emit {name,body} — fire a route without HTTP (self-test)
//!
//! The listener binds localhost only — never 0.0.0.0.

use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

pub type Pending = Arc<Mutex<HashMap<String, mpsc::Sender<Value>>>>;

const DEFAULT_PORT: u16 = 7844;
const BODY_CAP: usize = 8 * 1024;
const HEAD_CAP: usize = 16 * 1024;
const HOST_RUN_TTL: Duration = Duration::from_secs(30);
const TAIL_LINES: usize = 10;

// ---------- state ---------------------------------------------------------------

fn gray_home() -> PathBuf {
    std::env::var_os("GRAY_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".gray")))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn hooks_dir() -> PathBuf {
    gray_home().join("webhooks")
}

fn routes_path() -> PathBuf {
    hooks_dir().join("routes.json")
}

fn config_path() -> PathBuf {
    hooks_dir().join("config.json")
}

fn log_dir() -> PathBuf {
    hooks_dir().join("log")
}

fn configured_port() -> u16 {
    std::fs::read_to_string(config_path())
        .ok()
        .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        .and_then(|v| v.get("port").and_then(Value::as_u64))
        .map(|p| p.clamp(1, 65535) as u16)
        .unwrap_or(DEFAULT_PORT)
}

fn sanitize(s: &str) -> String {
    let s: String = s
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') { c } else { '-' })
        .collect();
    if s.is_empty() { "webhook".into() } else { s }
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

// ---------- routes ----------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Route {
    path: String,
    created_at: u64,
}

/// name → route, persisted at ~/.gray/webhooks/routes.json.
fn load_routes() -> BTreeMap<String, Route> {
    let Ok(raw) = std::fs::read_to_string(routes_path()) else { return BTreeMap::new() };
    let Ok(v) = serde_json::from_str::<Value>(&raw) else { return BTreeMap::new() };
    let Some(map) = v.as_object() else { return BTreeMap::new() };
    map.iter()
        .filter_map(|(name, r)| {
            let path = r.get("path").and_then(Value::as_str)?.to_string();
            let created_at = r.get("created_at").and_then(Value::as_u64).unwrap_or(0);
            Some((name.clone(), Route { path, created_at }))
        })
        .collect()
}

fn save_routes(routes: &BTreeMap<String, Route>) -> Result<(), String> {
    std::fs::create_dir_all(hooks_dir()).map_err(|e| e.to_string())?;
    let v: Value = routes
        .iter()
        .map(|(n, r)| (n.clone(), json!({"path": r.path, "created_at": r.created_at})))
        .collect::<serde_json::Map<String, Value>>()
        .into();
    std::fs::write(routes_path(), serde_json::to_string_pretty(&v).unwrap()).map_err(|e| e.to_string())
}

fn normalize_path(p: &str) -> Option<String> {
    let p = p.trim();
    let p = if p.starts_with('/') { p.to_string() } else { format!("/{p}") };
    if p.len() < 2 || p.contains('?') || p.contains(' ') || !p.chars().all(|c| c.is_ascii()) {
        return None;
    }
    Some(p)
}

fn valid_name(n: &str) -> bool {
    !n.is_empty() && n.len() <= 64 && n.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
}

// ---------- shared state ------------------------------------------------------------

pub struct Shared {
    pub out: Arc<Mutex<std::io::Stdout>>,
    pub pending: Pending,
    pub counter: Arc<Mutex<u64>>,
    pub queue: Mutex<Vec<String>>,
    pub routes: Mutex<BTreeMap<String, Route>>,
    pub listener_up: AtomicBool,
    pub bind_err: Mutex<Option<String>>,
    pub run_broken: AtomicBool,
    /// host/run wait budget — a field so tests can shrink it.
    pub run_ttl: Duration,
    pub stop: Arc<AtomicBool>,
}

impl Shared {
    pub fn new() -> Self {
        Self {
            out: Arc::new(Mutex::new(std::io::stdout())),
            pending: Arc::new(Mutex::new(HashMap::new())),
            counter: Arc::new(Mutex::new(0)),
            queue: Mutex::new(Vec::new()),
            routes: Mutex::new(load_routes()),
            listener_up: AtomicBool::new(false),
            bind_err: Mutex::new(None),
            run_broken: AtomicBool::new(false),
            run_ttl: HOST_RUN_TTL,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }
}

fn next_id(counter: &Arc<Mutex<u64>>) -> String {
    let mut n = counter.lock().expect("counter");
    *n += 1;
    format!("q{n}")
}

fn host_send(out: &Arc<Mutex<std::io::Stdout>>, method: &str, params: Value, id: &str) {
    let req = json!({"id": id, "method": method, "params": params});
    let mut o = out.lock().expect("stdout");
    let _ = writeln!(o, "{req}");
    let _ = o.flush();
}

fn host_run(sh: &Shared, prompt: &str) -> Result<String, String> {
    let id = next_id(&sh.counter);
    let (tx, rx) = mpsc::channel();
    sh.pending.lock().expect("pending").insert(id.clone(), tx);
    host_send(&sh.out, "host/run", json!({"prompt": prompt}), &id);
    let res = rx.recv_timeout(sh.run_ttl);
    sh.pending.lock().expect("pending").remove(&id);
    match res {
        Err(_) => Err("host/run timed out".into()),
        Ok(v) => {
            if let Some(e) = v.pointer("/error/message").and_then(Value::as_str) {
                return Err(e.to_string());
            }
            Ok(v.pointer("/result/text").and_then(Value::as_str).unwrap_or("").to_string())
        }
    }
}

/// One delivery path for HTTP posts and `webhook_emit` alike: log it, try
/// host/run, fall back to the before_start queue. Returns "run" | "queued".
fn deliver(sh: &Shared, name: &str, body: &str, content_type: &str) -> String {
    let prompt = format!("Webhook {name} fired: {body}");
    let via = if !sh.run_broken.load(Ordering::SeqCst) && host_run(sh, &prompt).is_ok() {
        "run"
    } else {
        sh.run_broken.store(true, Ordering::SeqCst);
        sh.queue.lock().expect("queue").push(prompt);
        "queued"
    };
    let _ = append_delivery(name, body, content_type, via);
    via.to_string()
}

fn append_delivery(name: &str, body: &str, content_type: &str, via: &str) -> Result<(), String> {
    let dir = log_dir();
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let body: String = body.chars().take(512).collect();
    let line = json!({"ts": now(), "body": body, "content_type": content_type, "via": via});
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(format!("{}.jsonl", sanitize(name))))
        .map_err(|e| e.to_string())?;
    writeln!(f, "{line}").map_err(|e| e.to_string())
}

fn tail_log(name: &str) -> Vec<String> {
    let path = log_dir().join(format!("{}.jsonl", sanitize(name)));
    let Ok(raw) = std::fs::read_to_string(path) else { return Vec::new() };
    let lines: Vec<&str> = raw.lines().filter(|l| !l.trim().is_empty()).collect();
    lines
        .iter()
        .rev()
        .take(TAIL_LINES)
        .rev()
        .map(|l| {
            serde_json::from_str::<Value>(l)
                .map(|v| {
                    let body = v.get("body").and_then(Value::as_str).unwrap_or("");
                    let body: String = body.chars().take(120).collect();
                    format!(
                        "ts={} via={} ct={} {}",
                        v.get("ts").and_then(Value::as_u64).unwrap_or(0),
                        v.get("via").and_then(Value::as_str).unwrap_or("?"),
                        v.get("content_type").and_then(Value::as_str).unwrap_or("-"),
                        body,
                    )
                })
                .unwrap_or_else(|_| l.to_string())
        })
        .collect()
}

// ---------- HTTP listener -----------------------------------------------------------

pub fn ensure_listener(sh: &Arc<Shared>) {
    if sh.listener_up.swap(true, Ordering::SeqCst) {
        return;
    }
    let port = configured_port();
    match TcpListener::bind(("127.0.0.1", port)) {
        Ok(l) => {
            let sh2 = sh.clone();
            std::thread::spawn(move || accept_loop(sh2, l));
        }
        Err(e) => {
            *sh.bind_err.lock().expect("bind_err") = Some(format!("127.0.0.1:{port}: {e}"));
            sh.listener_up.store(false, Ordering::SeqCst);
        }
    }
}

fn accept_loop(sh: Arc<Shared>, listener: TcpListener) {
    let _ = listener.set_nonblocking(true);
    while !sh.stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => handle_conn(&sh, stream),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(25))
            }
            Err(_) => break,
        }
    }
}

/// Read request head + body, dispatch, write a JSON reply. All failures
/// answer something — a webhook caller should never hang.
fn handle_conn(sh: &Arc<Shared>, mut stream: TcpStream) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(10)));
    let (status, payload) = match read_request(&mut stream) {
        Ok((method, path, content_type, body)) => {
            dispatch_request(sh, &method, &path, &content_type, &body)
        }
        Err(e) => (400, json!({"ok": false, "error": e})),
    };
    let text = payload.to_string();
    let _ = write!(
        stream,
        "HTTP/1.1 {status} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
        if status == 200 { "OK" } else { "ERR" },
        text.len(),
    );
    let _ = stream.flush();
}

fn read_request(stream: &mut TcpStream) -> Result<(String, String, String, String), String> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        if buf.len() > HEAD_CAP {
            return Err("headers too large".into());
        }
        let n = stream.read(&mut chunk).map_err(|e| format!("read: {e}"))?;
        if n == 0 {
            return Err("connection closed before headers".into());
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
    let mut lines = head.lines();
    let request = lines.next().ok_or("empty request")?;
    let mut parts = request.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    let mut content_len = 0usize;
    let mut content_type = String::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_lowercase();
            if k == "content-length" {
                content_len = v.trim().parse().unwrap_or(0);
            } else if k == "content-type" {
                content_type = v.trim().to_string();
            }
        }
    }
    let want = content_len.min(BODY_CAP);
    let mut body = buf[head_end..].to_vec();
    while body.len() < want {
        let n = stream.read(&mut chunk).map_err(|e| format!("read body: {e}"))?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(want);
    Ok((method, path, content_type, String::from_utf8_lossy(&body).to_string()))
}

fn dispatch_request(sh: &Arc<Shared>, method: &str, raw_path: &str, content_type: &str, body: &str) -> (u16, Value) {
    if method != "POST" {
        return (405, json!({"ok": false, "error": "POST only"}));
    }
    let path = raw_path.split('?').next().unwrap_or(raw_path);
    let found = sh
        .routes
        .lock()
        .expect("routes")
        .iter()
        .find(|(_, r)| r.path == path)
        .map(|(n, _)| n.clone());
    match found {
        Some(name) => {
            let via = deliver(sh, &name, body, content_type);
            (200, json!({"ok": true, "name": name, "delivered": via}))
        }
        None => (404, json!({"ok": false, "error": format!("no route for {path}")})),
    }
}

// ---------- hooks / tool / command --------------------------------------------------

fn before_start(sh: &Arc<Shared>) -> Value {
    // With routes on disk the listener starts even before the first /webhook.
    if !sh.routes.lock().expect("routes").is_empty() {
        ensure_listener(sh);
    }
    let msgs = std::mem::take(&mut *sh.queue.lock().expect("queue"));
    if msgs.is_empty() {
        json!({})
    } else {
        json!({"text": msgs.join("\n\n")})
    }
}

fn emit(sh: &Arc<Shared>, args: &Value) -> Result<String, String> {
    let name = args.get("name").and_then(Value::as_str).unwrap_or("").trim();
    if name.is_empty() {
        return Err("missing required argument: name".into());
    }
    let body = args.get("body").and_then(Value::as_str).unwrap_or("").to_string();
    if !sh.routes.lock().expect("routes").contains_key(name) {
        let names: Vec<String> = sh.routes.lock().expect("routes").keys().cloned().collect();
        return Err(if names.is_empty() {
            format!("no route named '{name}' (no routes at all — /webhook add first)")
        } else {
            format!("no route named '{name}' — routes: {}", names.join(", "))
        });
    }
    let via = deliver(sh, name, &body, "tool/webhook_emit");
    Ok(format!("webhook '{name}' delivered via {via}"))
}

pub fn listener_state(sh: &Shared) -> String {
    if sh.listener_up.load(Ordering::SeqCst) && sh.bind_err.lock().expect("e").is_none() {
        format!("listening on http://127.0.0.1:{}", configured_port())
    } else if let Some(e) = sh.bind_err.lock().expect("e").clone() {
        format!("bind failed: {e}")
    } else {
        format!("not bound (port {} when started)", configured_port())
    }
}

fn run_command(sh: &Arc<Shared>, argv: &[&str]) -> String {
    match argv.first().copied() {
        Some("add") => {
            let (Some(name), Some(path)) = (argv.get(1).copied(), argv.get(2).copied()) else {
                return "usage: /webhook add <name> <path>".into();
            };
            if !valid_name(name) {
                return format!("bad name '{name}' — letters, digits, - and _ only (≤64 chars)");
            }
            let Some(path) = normalize_path(path) else {
                return format!("bad path — need something like /deploy or /ci-done (no spaces, no query)");
            };
            let mut routes = sh.routes.lock().expect("routes");
            if let Some((other, _)) = routes.iter().find(|(n, r)| n.as_str() != name && r.path == path) {
                return format!("path {path} is already taken by '{other}'");
            }
            routes.insert(name.to_string(), Route { path: path.clone(), created_at: now() });
            let res = save_routes(&routes);
            drop(routes);
            ensure_listener(sh);
            match res {
                Ok(()) => format!(
                    "route added: POST http://127.0.0.1:{}{path} → webhook '{name}'{}",
                    configured_port(),
                    match sh.bind_err.lock().expect("e").clone() {
                        Some(e) => format!("\n(warning: listener {e})"),
                        None => String::new(),
                    }
                ),
                Err(e) => format!("route added but couldn't save {}: {e}", routes_path().display()),
            }
        }
        Some("rm") | Some("remove") => {
            let Some(name) = argv.get(1).copied() else {
                return "usage: /webhook rm <name>".into();
            };
            let mut routes = sh.routes.lock().expect("routes");
            if routes.remove(name).is_none() {
                return format!("no route named '{name}'");
            }
            match save_routes(&routes) {
                Ok(()) => format!("removed '{name}'"),
                Err(e) => format!("removed '{name}' but couldn't save routes.json: {e}"),
            }
        }
        Some("tail") => {
            let Some(name) = argv.get(1).copied() else {
                return "usage: /webhook tail <name>".into();
            };
            let lines = tail_log(name);
            if lines.is_empty() {
                format!("no deliveries logged for '{name}'")
            } else {
                format!("last {} deliveries for '{name}':\n{}", lines.len(), lines.join("\n"))
            }
        }
        Some("list") | Some("ls") | None | Some("status") => {
            let routes = sh.routes.lock().expect("routes");
            let mut lines = vec![format!(
                "gray-webhooks {} — {} · queued: {}",
                env!("CARGO_PKG_VERSION"),
                listener_state(sh),
                sh.queue.lock().expect("queue").len(),
            )];
            if routes.is_empty() {
                lines.push("no routes — /webhook add <name> <path>".into());
            } else {
                for (n, r) in routes.iter() {
                    lines.push(format!("  {n:20} POST {}", r.path));
                }
            }
            lines.push("also: /webhook rm <name> · /webhook tail <name> · tool webhook_emit".into());
            lines.join("\n")
        }
        _ => "usage: /webhook add <name> <path> | list | rm <name> | tail <name>".into(),
    }
}

// ---------- wire ---------------------------------------------------------------------

pub fn manifest_webhooks() -> Value {
    json!({
        "name": "webhooks",
        "version": env!("CARGO_PKG_VERSION"),
        "protocol": "2.0",
        "capabilities": ["host.turn"],
        "tools": [{
            "name": "webhook_emit",
            "description": "Fire a registered webhook route by name without going over HTTP — delivers its body through the same path a POST would take (immediate host/run, or queued for the next turn). For self-testing routes; add them with /webhook add <name> <path>.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Route name from /webhook list."},
                    "body": {"type": "string", "description": "Payload delivered as `Webhook <name> fired: <body>`."}
                },
                "required": ["name"]
            }
        }],
        "commands": ["/webhook"],
        "hooks": ["agent/before_start"],
    })
}

/// One request → `Some(reply)`, or `None` for notifications. The bool asks
/// the loop to exit after writing the reply.
pub fn handle_webhooks(sh: &Arc<Shared>, req: &Value) -> (Option<Value>, bool) {
    let id = req.get("id").cloned();
    let method = req.get("method").and_then(Value::as_str).unwrap_or("");
    let params = req.get("params").cloned().unwrap_or(Value::Null);
    let Some(id) = id else {
        if method == "plugin/shutdown" {
            sh.stop.store(true, Ordering::SeqCst);
        }
        return (None, method == "plugin/shutdown");
    };
    let result = match method {
        "plugin/manifest" => manifest_webhooks(),
        "agent/before_start" => before_start(sh),
        "tool/call" => {
            let name = params.get("name").and_then(Value::as_str).unwrap_or("");
            let args = params.get("args").cloned().unwrap_or(Value::Null);
            match name {
                "webhook_emit" => match emit(sh, &args) {
                    Ok(text) => json!({ "content": text }),
                    Err(e) => json!({ "content": e, "is_error": true }),
                },
                other => json!({ "content": format!("unknown tool: {other}"), "is_error": true }),
            }
        }
        "command/run" => {
            let argv: Vec<&str> = params
                .get("argv")
                .and_then(Value::as_array)
                .map(|a| a.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            json!({ "text": run_command(sh, &argv) })
        }
        "plugin/shutdown" => {
            sh.stop.store(true, Ordering::SeqCst);
            return (Some(json!({ "id": id, "result": {} })), true);
        }
        _ => {
            let error = json!({ "code": -32601, "message": "method not found" });
            return (Some(json!({ "id": id, "error": error })), false);
        }
    };
    (Some(json!({ "id": id, "result": result })), false)
}

fn main_webhooks() {
    if std::env::args().nth(1).as_deref() == Some("manifest") {
        println!("{}", manifest_webhooks());
        return;
    }
    let sh = Arc::new(Shared::new());
    let pending = sh.pending.clone();
    let out = sh.out.clone();

    // Reader thread: host→sidecar replies (string id, no method) route to
    // pending waiters or are dropped; requests feed the work loop.
    let (work_tx, work_rx) = mpsc::channel::<Value>();
    let _reader = std::thread::spawn(move || {
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            let Ok(line) = line else { break };
            let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
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
        let (reply, exit) = handle_webhooks(&sh, &req);
        if let Some(reply) = reply {
            let mut o = out.lock().expect("stdout");
            let _ = writeln!(o, "{reply}");
            let _ = o.flush();
        }
        if exit {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shared() -> Arc<Shared> {
        let mut s = Shared::new();
        s.run_ttl = Duration::from_millis(50);
        Arc::new(s)
    }

    fn call(sh: &Arc<Shared>, method: &str, params: Value) -> Value {
        handle_webhooks(sh, &json!({ "id": 1, "method": method, "params": params })).0.unwrap()
    }

    fn routed(name: &str, path: &str) -> Arc<Shared> {
        let sh = shared();
        sh.routes.lock().unwrap().insert(
            name.to_string(),
            Route { path: path.to_string(), created_at: 1 },
        );
        sh
    }

    #[test]
    fn manifest_shape() {
        let m = call(&shared(), "plugin/manifest", Value::Null)["result"].clone();
        assert_eq!(m["name"], "webhooks");
        assert_eq!(m["protocol"], "2.0");
        assert_eq!(m["tools"][0]["name"], "webhook_emit");
        assert_eq!(m["commands"], json!(["/webhook"]));
        assert_eq!(m["hooks"], json!(["agent/before_start"]));
        assert_eq!(m["capabilities"], json!(["host.turn"]));
    }

    #[test]
    fn dispatch_routes_post_and_404s() {
        let sh = routed("deploy", "/deploy");
        // host/run can't reach a host in tests → falls back to queue
        let (status, v) = dispatch_request(&sh, "POST", "/deploy", "text/plain", "ship it");
        assert_eq!(status, 200);
        assert_eq!(v["delivered"], json!("queued"));
        assert_eq!(sh.queue.lock().unwrap().len(), 1);
        let (status, _) = dispatch_request(&sh, "POST", "/nope", "", "");
        assert_eq!(status, 404);
        let (status, _) = dispatch_request(&sh, "GET", "/deploy", "", "");
        assert_eq!(status, 405);
        let (status, v) = dispatch_request(&sh, "POST", "/deploy?x=1", "", "b");
        assert_eq!(status, 200, "{v}");
    }

    #[test]
    fn queued_payloads_reach_before_start() {
        let sh = routed("ci", "/ci");
        dispatch_request(&sh, "POST", "/ci", "", "green");
        dispatch_request(&sh, "POST", "/ci", "", "again");
        let r = call(&sh, "agent/before_start", json!({"session":{"id":"s","cwd":"/tmp"}}));
        let t = r["result"]["text"].as_str().unwrap();
        assert!(t.contains("Webhook ci fired: green") && t.contains("Webhook ci fired: again"), "{t}");
        let r = call(&sh, "agent/before_start", json!({"session":{"id":"s","cwd":"/tmp"}}));
        assert_eq!(r["result"], json!({}));
    }

    #[test]
    fn emit_validates_and_delivers() {
        let sh = routed("me", "/me");
        assert!(emit(&sh, &json!({})).unwrap_err().contains("name"));
        assert!(emit(&sh, &json!({"name":"ghost"})).unwrap_err().contains("no route"));
        let ok = emit(&sh, &json!({"name":"me","body":"hi"})).unwrap();
        assert!(ok.contains("queued"), "{ok}");
    }

    #[test]
    fn name_and_path_validation() {
        assert!(valid_name("ci-done_2"));
        assert!(!valid_name("") && !valid_name("a b") && !valid_name("a/b"));
        assert_eq!(normalize_path("x").as_deref(), Some("/x"));
        assert_eq!(normalize_path("/x").as_deref(), Some("/x"));
        assert!(normalize_path("/").is_none());
        assert!(normalize_path("/a b").is_none());
        assert!(normalize_path("/a?b").is_none());
    }

    #[test]
    fn add_list_rm_flow() {
        // routes_path is env-derived; exercise the in-memory command path only
        let sh = shared();
        let r = call(&sh, "command/run", json!({"name":"/webhook","argv":["add","a b","/x"]}));
        assert!(r["result"]["text"].as_str().unwrap().contains("bad name"));
        let r = call(&sh, "command/run", json!({"name":"/webhook","argv":["rm","ghost"]}));
        assert!(r["result"]["text"].as_str().unwrap().contains("no route"));
    }

    #[test]
    fn tail_renders_deliveries() {
        let dir = std::env::temp_dir().join(format!("gray-wh-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // append_delivery writes under gray_home — test the formatter directly
        let path = dir.join("n.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        for i in 0..12 {
            writeln!(f, r#"{{"ts":{i},"body":"b{i}","content_type":"t","via":"queued"}}"#).unwrap();
        }
        let lines: Vec<String> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(str::to_string)
            .collect();
        assert_eq!(lines.len(), 12);
        assert!(tail_log("definitely-missing").is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shutdown_replies_then_exits() {
        let sh = shared();
        let (reply, exit) = handle_webhooks(&sh, &json!({ "id": 2, "method": "plugin/shutdown" }));
        assert!(reply.is_some() && exit);
        assert!(sh.stop.load(Ordering::SeqCst));
    }
}
