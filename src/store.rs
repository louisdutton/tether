//! Tether-owned identities and transcripts. Native IDs never serve as public IDs.
use crate::model::{Item, Session, TranscriptTurn, Turn, TurnContent};
use anyhow::{Context, Result};
use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use uuid::Uuid;

pub struct Store(Connection);

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty())
            && !parent.exists()
        {
            std::fs::create_dir_all(parent)?;
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        }
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)?;
        let db = Connection::open(path)?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        db.execute_batch(
            "PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS catalog_snapshots (id TEXT PRIMARY KEY, body TEXT NOT NULL, created_at INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS turns (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE, session_id TEXT NOT NULL,
                provider TEXT NOT NULL, native_session TEXT NOT NULL, native_turn TEXT NOT NULL,
                body TEXT NOT NULL,
                UNIQUE(session_id, provider, native_session, native_turn));
            CREATE TABLE IF NOT EXISTS queued_messages (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE, session_id TEXT NOT NULL,
                state TEXT NOT NULL, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS monitoring_events (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL, turn_id TEXT NOT NULL, kind TEXT NOT NULL,
                UNIQUE(session_id, turn_id, kind));
            CREATE TABLE IF NOT EXISTS inputs (
                session_id TEXT NOT NULL, provider TEXT NOT NULL,
                native_session TEXT NOT NULL, native_turn TEXT NOT NULL, text TEXT NOT NULL,
                PRIMARY KEY(session_id, provider, native_session, native_turn));",
        )?;
        let has_content = db
            .prepare("PRAGMA table_info(inputs)")?
            .query_map([], |r| r.get::<_, String>(1))?
            .collect::<std::result::Result<Vec<_>, _>>()?
            .iter()
            .any(|name| name == "content");
        if !has_content {
            db.execute("ALTER TABLE inputs ADD COLUMN content TEXT", [])?;
        }
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        // ACP exposes no receipt/turn lookup for an interrupted execution-server
        // lifetime. Preserve the input and mark unsettled work as ambiguous.
        let incomplete = db
            .prepare("SELECT id,body,session_id FROM turns")?
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        for (id, body, session) in incomplete {
            let mut body: serde_json::Value = serde_json::from_str(&body)?;
            if body["status"] == "inProgress" {
                db.execute("UPDATE queued_messages SET state='paused' WHERE session_id=?1 AND state='queued'", [&session])?;
                body["status"] = "failed".into();
                body["error"] = serde_json::json!({"message":"execution server restarted before turn settlement; delivery is unknown"});
                db.execute(
                    "UPDATE turns SET body=?1 WHERE id=?2",
                    params![body.to_string(), id],
                )?;
            }
        }
        // A crash after dispatch began has an unknown receipt. Never resend it.
        db.execute(
            "UPDATE queued_messages SET state='uncertain' WHERE state='dispatching'",
            [],
        )?;
        Ok(Self(db))
    }

    pub fn queue_message(&self, message: &crate::model::QueuedMessage) -> Result<()> {
        self.0.execute(
            "INSERT OR IGNORE INTO queued_messages(id,session_id,state,body) VALUES (?1,?2,?3,?4)",
            params![
                message.message_id,
                message.session_id,
                message.state,
                serde_json::to_string(message)?
            ],
        )?;
        Ok(())
    }
    pub fn queued(&self, session: Option<&str>) -> Result<Vec<crate::model::QueuedMessage>> {
        let mut query = self.0.prepare("SELECT body,state FROM queued_messages WHERE (?1 IS NULL OR session_id=?1) ORDER BY sequence")?;
        query
            .query_map([session], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .map(|r| {
                let (body, state) = r?;
                let mut message: crate::model::QueuedMessage = serde_json::from_str(&body)?;
                message.state = state;
                Ok(message)
            })
            .collect()
    }
    pub fn queue_state(&self, id: &str, state: &str, error: Option<&str>) -> Result<()> {
        self.0.execute(
            "UPDATE queued_messages SET state=?2,body=json_set(body,'$.error',?3) WHERE id=?1",
            params![id, state, error],
        )?;
        Ok(())
    }
    pub fn pause_queue(&self, session: &str) -> Result<()> {
        self.0.execute(
            "UPDATE queued_messages SET state='paused' WHERE session_id=?1 AND state='queued'",
            [session],
        )?;
        Ok(())
    }

    pub fn transaction<T>(&self, f: impl FnOnce(&Self) -> Result<T>) -> Result<T> {
        let transaction = self.0.unchecked_transaction()?;
        let result = f(self)?;
        transaction.commit()?;
        Ok(result)
    }
    pub fn save_with_history(&self, session: &Session, turns: &[Turn]) -> Result<()> {
        self.transaction(|db| {
            db.save(session)?;
            let b = session.binding();
            for turn in turns {
                db.put_turn(&session.id, &b.provider, &b.session_id, turn)?;
            }
            Ok(())
        })
    }

    pub fn save(&self, session: &Session) -> Result<()> {
        let mut body = serde_json::to_value(session)?;
        // Hidden from HTTP responses, but required to recover a prepared switch.
        body["pending_context"] = serde_json::to_value(&session.pending_context)?;
        body["pending_turn"] = serde_json::to_value(&session.pending_turn)?;
        body["mcp_servers"] = serde_json::to_value(&session.mcp_servers)?;
        self.0.execute(
            "INSERT INTO sessions VALUES (?1,?2)
            ON CONFLICT(id) DO UPDATE SET body=excluded.body",
            params![session.id, body.to_string()],
        )?;
        Ok(())
    }

    pub fn delete(&self, id: &str) -> Result<()> {
        self.transaction(|db| {
            db.0.execute("DELETE FROM queued_messages WHERE session_id=?1", [id])?;
            db.0.execute("DELETE FROM inputs WHERE session_id=?1", [id])?;
            db.0.execute("DELETE FROM turns WHERE session_id=?1", [id])?;
            db.0.execute("DELETE FROM sessions WHERE id=?1", [id])?;
            Ok(())
        })
    }

    pub fn get(&self, id: &str) -> Result<Option<Session>> {
        self.0
            .query_row("SELECT body FROM sessions WHERE id=?1", [id], |r| {
                r.get::<_, String>(0)
            })
            .optional()?
            .map(|s| serde_json::from_str(&s).context("reading session"))
            .transpose()
    }

    pub fn sessions(&self) -> Result<Vec<Session>> {
        let mut query = self.0.prepare("SELECT body FROM sessions ORDER BY rowid")?;
        query
            .query_map([], |r| r.get::<_, String>(0))?
            .map(|r| Ok(serde_json::from_str(&r?)?))
            .collect()
    }

    pub fn save_catalog(&self, id: &str, snapshot: &crate::model::CatalogSnapshot) -> Result<()> {
        self.transaction(|db| {
            db.0.execute("DELETE FROM catalog_snapshots WHERE created_at < unixepoch() - 1800", [])?;
            db.0.execute("INSERT INTO catalog_snapshots VALUES (?1, ?2, unixepoch())",
                params![id, serde_json::to_string(snapshot)?])?;
            db.0.execute("DELETE FROM catalog_snapshots WHERE id NOT IN (SELECT id FROM catalog_snapshots ORDER BY created_at DESC, rowid DESC LIMIT 128)", [])?;
            Ok(())
        })
    }
    pub fn catalog(&self, id: &str) -> Result<Option<crate::model::CatalogSnapshot>> {
        let body: Option<String> = self.0.query_row(
            "SELECT body FROM catalog_snapshots WHERE id=?1 AND created_at >= unixepoch() - 1800",
            [id], |r| r.get(0)).optional()?;
        body.map(|s| Ok(serde_json::from_str(&s)?)).transpose()
    }

    pub fn owner(&self, provider: &str, native: &str) -> Result<Option<Session>> {
        Ok(self.sessions()?.into_iter().find(|s| {
            s.bindings
                .iter()
                .any(|b| b.provider == provider && b.session_id == native)
        }))
    }

    pub fn input(
        &self,
        session: &Session,
        native_turn: &str,
        text: &str,
        content: &[agent_client_protocol::v1::schema::ContentBlock],
    ) -> Result<()> {
        let binding = session.binding();
        self.0.execute(
            "INSERT INTO inputs(session_id,provider,native_session,native_turn,text,content) VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                session.id,
                binding.provider,
                binding.session_id,
                native_turn,
                text,
                serde_json::to_string(content)?
            ],
        )?;
        Ok(())
    }

    pub fn put_turn(
        &self,
        session_id: &str,
        provider: &str,
        native: &str,
        turn: &Turn,
    ) -> Result<TranscriptTurn> {
        let existing = self.native_turn(session_id, provider, native, &turn.id)?;
        let mut items = turn.items.clone();
        let input: Option<String> = self.0.query_row(
            "SELECT text FROM inputs WHERE session_id=?1 AND provider=?2 AND native_session=?3 AND native_turn=?4",
            params![session_id, provider, native, turn.id], |r| r.get(0)).optional()?;
        if let Some(text) = input {
            // Keep the authored message in shared history, not the injected replay.
            items.retain(|i| i.kind != "message.user" || i.id.starts_with("steer:"));
            let content: Option<String> = self.0.query_row("SELECT content FROM inputs WHERE session_id=?1 AND provider=?2 AND native_session=?3 AND native_turn=?4", params![session_id, provider, native, turn.id], |r| r.get(0))?;
            let blocks: Vec<agent_client_protocol::v1::schema::ContentBlock> = content
                .map(|v| serde_json::from_str(&v))
                .transpose()?
                .unwrap_or_else(|| vec![text.clone().into()]);
            for (index, block) in blocks.into_iter().enumerate().rev() {
                let text = match &block {
                    agent_client_protocol::v1::schema::ContentBlock::Text(t) => {
                        Some(t.text.clone())
                    }
                    _ => None,
                };
                items.insert(
                    0,
                    Item {
                        id: format!("user:{}:{index}", turn.id),
                        kind: "message.user".into(),
                        text,
                        provider_data: serde_json::to_value(block)?,
                    },
                );
            }
        }
        let content = TurnContent {
            status: turn.status.clone(),
            items,
            error: turn.error.clone(),
        };
        let id = existing
            .as_ref()
            .map(|t| t.id.clone())
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        self.0.execute(
            "INSERT INTO turns(id,session_id,provider,native_session,native_turn,body)
            VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(session_id,provider,native_session,native_turn)
            DO UPDATE SET body=excluded.body",
            params![
                id,
                session_id,
                provider,
                native,
                turn.id,
                serde_json::to_string(&content)?
            ],
        )?;
        self.native_turn(session_id, provider, native, &turn.id)?
            .context("saved turn missing")
    }

    pub fn native_turn(
        &self,
        session: &str,
        provider: &str,
        native: &str,
        turn: &str,
    ) -> Result<Option<TranscriptTurn>> {
        let row = self.0.query_row(
            "SELECT id,sequence,body FROM turns WHERE session_id=?1 AND provider=?2 AND native_session=?3 AND native_turn=?4",
            params![session,provider,native,turn],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?))).optional()?;
        row.map(|(id, sequence, body)| {
            Ok(TranscriptTurn {
                id,
                sequence,
                provider: provider.into(),
                provider_session_id: native.into(),
                provider_turn_id: turn.into(),
                content: serde_json::from_str(&body)?,
            })
        })
        .transpose()
    }

    /// A bounded excerpt of the last assistant message in this exact public turn.
    /// Never refresh native history or include tools, thoughts, or another turn.
    pub fn response_preview(&self, session: &str, turn: &str) -> Result<Option<String>> {
        let body: Option<String> = self
            .0
            .query_row(
                "SELECT body FROM turns WHERE session_id=?1 AND id=?2",
                params![session, turn],
                |r| r.get(0),
            )
            .optional()?;
        let content: Option<TurnContent> =
            body.map(|body| serde_json::from_str(&body)).transpose()?;
        Ok(content.and_then(|content| response_preview(&content.items)))
    }

    pub fn monitor_event(&self, event: &crate::model::Event) -> Result<()> {
        self.0.execute(
            "INSERT OR IGNORE INTO monitoring_events(session_id,turn_id,kind) VALUES(?1,?2,?3)",
            params![event.session_id, event.turn_id, event.kind],
        )?;
        self.0.execute("DELETE FROM monitoring_events WHERE sequence < (SELECT COALESCE(MAX(sequence),0)-1023 FROM monitoring_events)", [])?;
        Ok(())
    }
    pub fn monitor_events(&self, cursor: Option<u64>) -> Result<serde_json::Value> {
        let head: i64 = self.0.query_row(
            "SELECT COALESCE(MAX(sequence),0) FROM monitoring_events",
            [],
            |r| r.get(0),
        )?;
        let head = head as u64;
        let Some(cursor) = cursor.filter(|cursor| *cursor <= head) else {
            return Ok(serde_json::json!({"events":[],"cursor":head.to_string(),"more":false}));
        };
        let first: i64 = self.0.query_row(
            "SELECT COALESCE(MIN(sequence),0) FROM monitoring_events",
            [],
            |r| r.get(0),
        )?;
        let first = first as u64;
        let mut query = self.0.prepare("SELECT sequence,session_id,turn_id,kind FROM monitoring_events WHERE sequence>?1 ORDER BY sequence LIMIT 256")?;
        let mut last = cursor;
        let events = query
            .query_map([cursor as i64], |r| {
                Ok((
                    r.get::<_, i64>(0)? as u64,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })?
            .map(|row| {
                let (sequence, session, turn, kind) = row?;
                last = sequence;
                Ok(serde_json::json!({"session_id":session,"turn_id":turn,"kind":kind}))
            })
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(
            serde_json::json!({"events":events,"cursor":last.to_string(),"more":last<head,"truncated":cursor+1<first}),
        )
    }
    pub fn history(&self, session: &str) -> Result<Vec<TranscriptTurn>> {
        let mut query = self.0.prepare(
            "SELECT id,sequence,provider,native_session,native_turn,body
            FROM turns WHERE session_id=?1 ORDER BY sequence",
        )?;
        let rows = query.query_map([session], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
            ))
        })?;
        rows.map(|row| {
            let (id, sequence, provider, provider_session_id, provider_turn_id, body) = row?;
            Ok(TranscriptTurn {
                id,
                sequence,
                provider,
                provider_session_id,
                provider_turn_id,
                content: serde_json::from_str(&body)?,
            })
        })
        .collect()
    }
}

fn response_preview(items: &[Item]) -> Option<String> {
    let assistant = |item: &Item| {
        item.kind == "message.assistant"
            || (item.kind == "acp.update"
                && item.provider_data["sessionUpdate"] == "agent_message_chunk"
                && item.provider_data["content"]["type"] == "text")
    };
    let end = items.iter().rposition(|item| {
        assistant(item) && item.text.as_ref().is_some_and(|text| !text.is_empty())
    })?;
    let mut start = end;
    if items[end].kind == "acp.update" {
        while start > 0 && assistant(&items[start - 1]) && items[start - 1].kind == "acp.update" {
            start -= 1;
        }
    }
    let mut chars = items[start..=end]
        .iter()
        .filter_map(|item| item.text.as_deref())
        .flat_map(str::chars);
    let mut preview: String = chars.by_ref().take(2048).collect();
    if chars.next().is_some() {
        preview.push('…');
    }
    (!preview.trim().is_empty()).then_some(preview)
}

#[cfg(test)]
mod monitor_tests {
    use super::*;
    use crate::model::Event;
    #[test]
    fn response_previews_are_turn_scoped_and_survive_reopening() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("previews.sqlite3");
        let public;
        {
            let store = Store::open(&path).unwrap();
            let chunk = |id: &str, text: &str, update: &str| Item {
                id: id.into(),
                kind: "acp.update".into(),
                text: Some(text.into()),
                provider_data: serde_json::json!({"sessionUpdate":update,"content":{"type":"text","text":text}}),
            };
            let turn = Turn {
                id: "native".into(),
                status: "completed".into(),
                error: None,
                items: vec![
                    chunk("1", "Earlier commentary", "agent_message_chunk"),
                    chunk("2", "secret thought", "agent_thought_chunk"),
                    chunk("3", "Final ", "agent_message_chunk"),
                    chunk("4", "answer 😀", "agent_message_chunk"),
                ],
            };
            public = store
                .put_turn("s", "codex", "native-session", &turn)
                .unwrap()
                .id;
            assert_eq!(
                store.response_preview("s", &public).unwrap().as_deref(),
                Some("Final answer 😀")
            );
            assert_eq!(
                store.response_preview("other-session", &public).unwrap(),
                None
            );
            assert_eq!(store.response_preview("s", "other-turn").unwrap(), None);
        }
        let store = Store::open(&path).unwrap();
        assert_eq!(
            store.response_preview("s", &public).unwrap().as_deref(),
            Some("Final answer 😀")
        );
    }

    #[test]
    fn response_previews_exclude_tool_and_thought_text_and_bound_unicode() {
        let item = |kind: &str, text: String| Item {
            id: kind.into(),
            kind: kind.into(),
            text: Some(text),
            provider_data: serde_json::Value::Null,
        };
        assert_eq!(
            response_preview(&[item("tool.activity", "private tool output".into())]),
            None
        );
        let preview = response_preview(&[
            item("message.assistant", "😀".repeat(3000)),
            item("tool.activity", "private tool output".into()),
        ])
        .unwrap();
        assert_eq!(preview.chars().count(), 2049);
        assert!(preview.ends_with("😀…"));
    }

    #[test]
    fn monitoring_events_are_durable_deduplicated_and_bounded() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.sqlite3");
        {
            let store = Store::open(&path).unwrap();
            assert_eq!(
                store.monitor_events(None).unwrap()["events"],
                serde_json::json!([])
            );
            let event = Event {
                session_id: Some("s".into()),
                turn_id: Some("t".into()),
                kind: "turn.completed".into(),
                data: serde_json::json!({}),
            };
            store.monitor_event(&event).unwrap();
            store.monitor_event(&event).unwrap();
            assert_eq!(
                store.monitor_events(Some(0)).unwrap()["events"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
        }
        let store = Store::open(&path).unwrap();
        assert_eq!(
            store.monitor_events(Some(0)).unwrap()["events"][0]["turn_id"],
            "t"
        );
        for i in 0..1030 {
            store
                .monitor_event(&Event {
                    session_id: Some("s".into()),
                    turn_id: Some(format!("t{i}")),
                    kind: "turn.completed".into(),
                    data: serde_json::json!({}),
                })
                .unwrap();
        }
        let first = store.monitor_events(Some(0)).unwrap();
        assert_eq!(first["truncated"], true);
        assert_eq!(first["more"], true);
        let count: i64 = store
            .0
            .query_row("SELECT COUNT(*) FROM monitoring_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1024);
        assert_eq!(
            store.monitor_events(Some(u64::MAX)).unwrap()["events"],
            serde_json::json!([])
        );
    }
}
