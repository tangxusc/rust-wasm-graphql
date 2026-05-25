use serde::{Deserialize, Serialize};
use serde_json::Value;

mod bindings {
    wit_bindgen::generate!({
        world: "counter-aggregate",
        path: "wit",
    });
}

use bindings::exports::test::counter::aggregate::Guest;

#[derive(Debug, Serialize, Deserialize, Default)]
struct CounterState {
    count: u64,
}

fn load_state(data: &[u8]) -> Result<CounterState, String> {
    if data.is_empty() {
        return Ok(CounterState::default());
    }
    serde_json::from_slice(data).map_err(|e| format!("反序列化失败: {e}"))
}

struct Component;

impl Guest for Component {
    fn validate_increment(command: Vec<u8>) -> Result<(), String> {
        let params: serde_json::Value = serde_json::from_slice(&command)
            .map_err(|e| format!("反序列化失败: {e}"))?;
        let amount = params["amount"].as_u64().unwrap_or(0);
        if amount == 0 {
            return Err("数量必须大于0".into());
        }
        Ok(())
    }

    fn handle_increment(_state: Vec<u8>, command: Vec<u8>) -> Result<Vec<Vec<u8>>, String> {
        let params: serde_json::Value = serde_json::from_slice(&command)
            .map_err(|e| format!("反序列化失败: {e}"))?;

        let event = serde_json::json!({
            "type": "Incremented",
            "amount": params["amount"].as_u64().unwrap_or(0),
        });
        Ok(vec![serde_json::to_vec(&event).map_err(|e| e.to_string())?])
    }

    fn apply_events(snapshot: Vec<u8>, events: Vec<Vec<u8>>) -> Result<Vec<u8>, String> {
        let mut state: CounterState = load_state(&snapshot)?;
        for event_data in &events {
            let event: Value = serde_json::from_slice(event_data)
                .map_err(|e| format!("事件反序列化失败: {e}"))?;
            match event["type"].as_str() {
                Some("Incremented") => {
                    let amount = event["amount"].as_u64().unwrap_or(0);
                    state.count += amount;
                }
                _ => {}
            }
        }
        serde_json::to_vec(&state).map_err(|e| e.to_string())
    }
}

bindings::export!(Component with_types_in bindings);
