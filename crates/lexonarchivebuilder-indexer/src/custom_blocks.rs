// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

use ciborium::Value;

pub(crate) const REPLAY_JOURNAL_BLOCK_TYPE: &str = "lexonarchivebuilder.replay-journal";
pub(crate) const REPLAY_JOURNAL_MEDIA_TYPE: &str =
    "application/vnd.lexonarchivebuilder.replay-journal+cbor";

pub(crate) fn custom_block_payload(content: &Value) -> Result<(String, Vec<u8>), String> {
    let Value::Map(fields) = content else {
        return Err("custom block content must be a CBOR map".into());
    };
    let media_type = fields
        .iter()
        .find_map(|(key, value)| match (key, value) {
            (Value::Text(name), Value::Text(media_type)) if name == "media_type" => {
                Some(media_type.clone())
            }
            _ => None,
        })
        .ok_or_else(|| "custom block content is missing media_type".to_string())?;
    let body = fields
        .iter()
        .find_map(|(key, value)| match (key, value) {
            (Value::Text(name), Value::Bytes(body)) if name == "body" => Some(body.clone()),
            _ => None,
        })
        .ok_or_else(|| "custom block content is missing body".to_string())?;
    Ok((media_type, body))
}
