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
use std::fmt;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tantivy::query::Query;
use tantivy::schema::Schema;
use tantivy::directory::Directory;
use tantivy::{Searcher, Term};

/// A query type implemented outside Quickwit, reachable as
/// `{"type": "extension", "kind": <kind>, "payload": ...}` in a query AST.
pub trait QueryExtension: Send + Sync + 'static {
    /// The `kind` this extension answers to.
    fn kind(&self) -> &str;

    /// Build the tantivy query for `payload` against a split's schema.
    ///
    /// The query's `Query::query_terms` is honored like any tantivy query's: every term it
    /// reports is warmed (with positions when requested) before the query runs. Anything else the
    /// query needs to read must be fetched by the returned [`ExtensionWarmup`], because tantivy
    /// queries execute synchronously on data that is already local.
    fn build(&self, payload: &JsonValue, schema: &Schema) -> Result<ExtensionQueryBuild, String>;
}

/// What [`QueryExtension::build`] produces for one split.
pub struct ExtensionQueryBuild {
    pub query: Box<dyn Query>,
    /// Asynchronous preparation run after Quickwit's own warmup (so the terms reported by the
    /// query's `query_terms` are already local) and before the query executes.
    pub warmup: Option<Arc<dyn ExtensionWarmup>>,
    /// Terms that every matching document contains. When one of them is absent from a split,
    /// Quickwit skips the split without running the warmup or the query.
    pub required_terms: Vec<Term>,
}

/// Asynchronous per-split preparation of an extension query.
#[async_trait]
pub trait ExtensionWarmup: Send + Sync + 'static {
    /// `searcher` is the split's searcher. `split_directory` is the split's raw file directory:
    /// it opens the split's sidecar files (which, unlike tantivy's files, carry no tantivy
    /// footer, so `searcher.index().directory()` rejects them). Only asynchronous reads reach
    /// storage; synchronous reads succeed for bytes in the hotcache or already read in this
    /// request.
    async fn warm(&self, searcher: &Searcher, split_directory: &dyn Directory)
    -> anyhow::Result<()>;
}

/// Warmups collected while building one split's query. Handed from the query builder to the
/// split's leaf search.
#[derive(Clone, Default)]
pub struct ExtensionWarmups(pub Vec<Arc<dyn ExtensionWarmup>>);

impl fmt::Debug for ExtensionWarmups {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExtensionWarmups({})", self.0.len())
    }
}

impl PartialEq for ExtensionWarmups {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self
                .0
                .iter()
                .zip(&other.0)
                .all(|(left, right)| Arc::ptr_eq(left, right))
    }
}

impl Eq for ExtensionWarmups {}

static QUERY_EXTENSIONS: RwLock<Option<HashMap<String, Arc<dyn QueryExtension>>>> =
    RwLock::new(None);

/// Register a query extension under its `kind`, replacing any previous one with that kind.
/// Call before starting any searcher.
pub fn register_query_extension(extension: Arc<dyn QueryExtension>) {
    let mut registry = QUERY_EXTENSIONS
        .write()
        .expect("query extension registry poisoned");
    registry
        .get_or_insert_with(HashMap::new)
        .insert(extension.kind().to_string(), extension);
}

/// The extension registered for `kind`.
pub fn query_extension(kind: &str) -> Option<Arc<dyn QueryExtension>> {
    QUERY_EXTENSIONS
        .read()
        .expect("query extension registry poisoned")
        .as_ref()?
        .get(kind)
        .cloned()
}
