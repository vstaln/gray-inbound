use std::collections::BTreeMap;
use std::path::PathBuf;

use serde_json::{Value, json};

pub struct Part {
    pub name: &'static str,
    pub default: bool,
    pub description: &'static str,
}

pub struct Parts {
    on: BTreeMap<&'static str, bool>,
}

fn gray_home() -> PathBuf {
    if let Some(h) = std::env::var_os("GRAY_HOME") {
        return PathBuf::from(h);
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .unwrap_or_default();
    PathBuf::from(home).join(".gray")
}

fn overrides(bundle: &str) -> BTreeMap<String, bool> {
    std::fs::read_to_string(gray_home().join("plugins").join("parts.json"))
        .ok()
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .and_then(|v| v.get(bundle).cloned())
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default()
}

impl Parts {
    pub fn load(bundle: &str, all: &[Part]) -> Self {
        let o = overrides(bundle);
        Self {
            on: all
                .iter()
                .map(|p| (p.name, o.get(p.name).copied().unwrap_or(p.default)))
                .collect(),
        }
    }

    pub fn on(&self, name: &str) -> bool {
        self.on.get(name).copied().unwrap_or(false)
    }

    pub fn manifest(&self, all: &[Part]) -> Value {
        Value::Array(
            all.iter()
                .map(|p| {
                    json!({
                        "name": p.name,
                        "default": p.default,
                        "enabled": self.on(p.name),
                        "description": p.description,
                    })
                })
                .collect(),
        )
    }
}
