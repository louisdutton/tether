//! Requests belong to the agent turn, not to a phone connection.
use anyhow::{Result, bail, ensure};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Mutex};
use tokio::sync::oneshot;

struct Pending {
    request: Value,
    answer: oneshot::Sender<Value>,
}
#[derive(Default)]
pub struct Requests(Mutex<BTreeMap<String, Pending>>);
impl Requests {
    pub fn begin(
        &self,
        session: &str,
        method: &str,
        params: Value,
    ) -> Result<(String, oneshot::Receiver<Value>)> {
        ensure!(
            matches!(method, "session/request_permission" | "elicitation/create"),
            "unsupported request"
        );
        let mut pending = self.0.lock().unwrap();
        pending.retain(|_, p| !p.answer.is_closed());
        ensure!(pending.len() < 128, "too many pending requests");
        let id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        pending.insert(id.clone(), Pending { request: json!({"requestId":id,"sessionId":session,"method":method,"params":params}), answer: tx });
        Ok((id, rx))
    }
    pub fn list(&self) -> Vec<Value> {
        let mut pending = self.0.lock().unwrap();
        pending.retain(|_, p| !p.answer.is_closed());
        pending.values().map(|p| p.request.clone()).collect()
    }
    pub fn answer(&self, session: &str, id: &str, response: Value) -> Result<()> {
        let mut pending = self.0.lock().unwrap();
        let p = pending
            .get(id)
            .filter(|p| p.request["sessionId"] == session)
            .ok_or_else(|| anyhow::anyhow!("request no longer available"))?;
        if p.request["method"] == "session/request_permission" {
            let outcome = &response["outcome"];
            match outcome["outcome"].as_str() {
                Some("cancelled") => {}
                Some("selected") => ensure!(
                    p.request["params"]["options"]
                        .as_array()
                        .is_some_and(|options| options
                            .iter()
                            .any(|o| o["optionId"].is_string()
                                && o["optionId"] == outcome["optionId"])),
                    "unknown permission option"
                ),
                _ => bail!("invalid permission response"),
            }
        } else {
            ensure!(
                matches!(
                    response["action"].as_str(),
                    Some("accept" | "decline" | "cancel")
                ),
                "invalid answer action"
            );
            let _: agent_client_protocol::v1::schema::CreateElicitationResponse =
                serde_json::from_value(response.clone())?;
            if response["action"] == "accept" && p.request["params"]["mode"] == "form" {
                validate_form(
                    &p.request["params"]["requestedSchema"],
                    &response["content"],
                )?;
            }
        }
        let p = pending.remove(id).unwrap();
        p.answer
            .send(response)
            .map_err(|_| anyhow::anyhow!("request no longer available"))
    }
    pub fn forget(&self, id: &str) {
        self.0.lock().unwrap().remove(id);
    }
    pub fn cancel(&self, session: &str) {
        self.0
            .lock()
            .unwrap()
            .retain(|_, p| p.request["sessionId"] != session);
    }
}
fn validate_form(schema: &Value, content: &Value) -> Result<()> {
    let values = content
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("form content must be an object"))?;
    let properties = schema["properties"]
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("invalid form schema"))?;
    if let Some(required) = schema["required"].as_array() {
        ensure!(
            required
                .iter()
                .all(|key| key.as_str().is_some_and(|key| values.contains_key(key))),
            "required answer missing"
        );
    }
    for (key, value) in values {
        let field = properties
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("unknown form field"))?;
        match field["type"].as_str() {
            Some("string") => {
                let text = value
                    .as_str()
                    .ok_or_else(|| anyhow::anyhow!("expected text"))?;
                ensure!(
                    text.chars().count() >= field["minLength"].as_u64().unwrap_or(0) as usize
                        && text.chars().count()
                            <= field["maxLength"].as_u64().unwrap_or(u64::MAX) as usize,
                    "invalid text length"
                );
                if let Some(options) = field["enum"].as_array() {
                    ensure!(options.contains(value), "unknown answer");
                }
                if let Some(options) = field["oneOf"].as_array() {
                    ensure!(
                        options.iter().any(|o| o["const"] == *value),
                        "unknown answer"
                    );
                }
            }
            Some("boolean") => ensure!(value.is_boolean(), "expected boolean"),
            Some("number" | "integer") => {
                let n = value
                    .as_f64()
                    .ok_or_else(|| anyhow::anyhow!("expected number"))?;
                ensure!(
                    field["type"] != "integer" || value.is_i64() || value.is_u64(),
                    "expected integer"
                );
                ensure!(
                    n >= field["minimum"].as_f64().unwrap_or(f64::NEG_INFINITY)
                        && n <= field["maximum"].as_f64().unwrap_or(f64::INFINITY),
                    "number out of range"
                );
            }
            Some("array") => {
                let items = value
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("expected array"))?;
                ensure!(
                    items.len() >= field["minItems"].as_u64().unwrap_or(0) as usize
                        && items.len() <= field["maxItems"].as_u64().unwrap_or(u64::MAX) as usize,
                    "invalid selection count"
                );
                for item in items {
                    let mut wrapper = serde_json::Map::new();
                    wrapper.insert("item".into(), field["items"].clone());
                    validate_form(&json!({"properties":wrapper}), &json!({"item":item}))?;
                }
            }
            _ => bail!("unsupported form field"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn pending_requests_survive_readers_and_validate_ownership_and_options() {
        let requests = Requests::default();
        let (id, rx) = requests
            .begin(
                "native",
                "session/request_permission",
                json!({"options":[{"optionId":"once"}]}),
            )
            .unwrap();
        assert_eq!(requests.list().len(), 1);
        assert!(
            requests
                .answer("wrong", &id, json!({"outcome":{"outcome":"cancelled"}}))
                .is_err()
        );
        assert!(
            requests
                .answer(
                    "native",
                    &id,
                    json!({"outcome":{"outcome":"selected","optionId":"always"}})
                )
                .is_err()
        );
        assert_eq!(requests.list().len(), 1);
        let response = json!({"outcome":{"outcome":"selected","optionId":"once"}});
        requests.answer("native", &id, response.clone()).unwrap();
        assert_eq!(rx.await.unwrap(), response);
        assert!(requests.list().is_empty());
        assert!(requests.answer("native", &id, response).is_err());
    }
    #[test]
    fn cancelled_turns_and_closed_agent_requests_disappear() {
        let requests = Requests::default();
        let (_, rx) = requests
            .begin("native", "elicitation/create", json!({}))
            .unwrap();
        requests.cancel("native");
        assert!(requests.list().is_empty());
        drop(rx);
        let (_, rx) = requests
            .begin("native", "elicitation/create", json!({}))
            .unwrap();
        drop(rx);
        assert!(requests.list().is_empty());
    }
    #[test]
    fn required_values_and_options_are_validated() {
        let schema = json!({"properties":{"q":{"type":"string","oneOf":[{"const":"yes"},{"const":"no"}]}},"required":["q"]});
        assert!(validate_form(&schema, &json!({})).is_err());
        assert!(validate_form(&schema, &json!({"q":"maybe"})).is_err());
        assert!(validate_form(&schema, &json!({"q":"yes"})).is_ok());
    }
}
