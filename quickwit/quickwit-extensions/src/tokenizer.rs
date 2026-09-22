// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::RwLock;

use tantivy::tokenizer::TextAnalyzer;

static TOKENIZERS: RwLock<Option<HashMap<String, TextAnalyzer>>> = RwLock::new(None);

/// Register a Tantivy tokenizer under `name`, replacing any previous one with that name.
///
/// Call before building any doc mapper (indexer, merge executor, and searcher all build one
/// from `create_default_quickwit_tokenizer_manager`, which includes every name registered here).
/// `name` must not collide with a Quickwit built-in tokenizer name; a collision is dropped with
/// a `warn!` when the default tokenizer manager is built, not here, since two extensions could
/// otherwise race on the registry without either knowing about Quickwit's own names.
pub fn register_tokenizer(name: &str, analyzer: TextAnalyzer) {
    let mut registry = TOKENIZERS.write().expect("tokenizer registry poisoned");
    registry
        .get_or_insert_with(HashMap::new)
        .insert(name.to_string(), analyzer);
}

/// Every tokenizer registered through [`register_tokenizer`], in no particular order.
pub fn registered_tokenizers() -> Vec<(String, TextAnalyzer)> {
    TOKENIZERS
        .read()
        .expect("tokenizer registry poisoned")
        .as_ref()
        .map(|map| map.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}
