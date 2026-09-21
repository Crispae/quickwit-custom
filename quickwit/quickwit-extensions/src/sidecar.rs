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

use std::io;
use std::path::Path;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use serde_json::{Map, Value as JsonValue};

/// An extra per-split component whose row `i` belongs to the split's document `i`.
///
/// Rows are pushed in document-id order: Quickwit's indexer adds documents to a split one at
/// a time, in arrival order, and never re-sorts them.
pub trait SplitSidecar: Send + Sync + 'static {
    /// File name inside the split bundle. Must not collide with tantivy's own files; prefix it
    /// with the extension's name (e.g. `rustie.gph2`).
    fn file_name(&self) -> &str;

    /// Pull this extension's payload out of a raw JSON document, removing whatever it consumed
    /// so the (possibly strict) doc mapping never sees it. `None` = the document has no row
    /// content (the row is still created, empty, to keep alignment).
    fn extract(&self, doc: &mut Map<String, JsonValue>) -> Option<Bytes>;

    /// A writer for one new split.
    fn new_writer(&self) -> io::Result<Box<dyn SidecarWriter>>;

    /// Build the sidecar of a merged split.
    ///
    /// `sources` are in output row order (the order the search index stacks its segments); each
    /// source contributes only its `alive_docs`. Write the result to `out`.
    fn merge(&self, sources: &[SidecarMergeSource], out: &Path) -> io::Result<()>;

    /// Byte ranges of the finished file worth keeping in the split's hotcache (e.g. its
    /// index / trailer), so opening it at search time costs no extra request. The file is always
    /// listed in the hotcache, which is what makes it openable through the split's directory.
    fn hotcache_ranges(&self, _file: &[u8]) -> Vec<std::ops::Range<usize>> {
        Vec::new()
    }
}

/// Accumulates the rows of one split.
pub trait SidecarWriter: Send {
    /// Append the next row.
    fn push(&mut self, payload: Option<Bytes>) -> io::Result<()>;

    /// Rows pushed so far.
    fn num_rows(&self) -> u32;

    /// Write the finished component to `out`.
    fn finish(self: Box<Self>, out: &Path) -> io::Result<()>;
}

/// One input split of a merge.
pub struct SidecarMergeSource {
    /// The split's sidecar file contents, or `None` if it has none (splits created before the
    /// extension was enabled): its rows become empty rows.
    pub data: Option<Arc<dyn AsRef<[u8]> + Send + Sync>>,
    /// Documents in the split, including deleted ones.
    pub num_docs: u32,
    /// Ascending ids of the documents that survive the merge; `None` keeps all of them.
    pub alive_docs: Option<Vec<u32>>,
}

static SIDECARS: RwLock<Vec<Arc<dyn SplitSidecar>>> = RwLock::new(Vec::new());

/// Register a sidecar. Call before starting any indexing or merge actor. Registering a second
/// sidecar with the same file name replaces the first.
pub fn register_split_sidecar(sidecar: Arc<dyn SplitSidecar>) {
    let mut registry = SIDECARS.write().expect("sidecar registry poisoned");
    registry.retain(|existing| existing.file_name() != sidecar.file_name());
    registry.push(sidecar);
}

/// The registered sidecars, in registration order (the order of `ProcessedDoc` payloads).
pub fn split_sidecars() -> Vec<Arc<dyn SplitSidecar>> {
    SIDECARS.read().expect("sidecar registry poisoned").clone()
}
