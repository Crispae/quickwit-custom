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

//! Extension points for embedding applications.
//!
//! Quickwit itself never implements these traits. An application that links Quickwit
//! (indexer, searcher, ...) registers implementations once at start-up, before any actor
//! runs, and Quickwit's indexing / merge / search paths call into them.
//!
//! * [`SplitSidecar`]: an extra per-split file whose rows are aligned with the split's
//!   document ids. It is built while documents are indexed, rebuilt on merges (dropping deleted
//!   documents), shipped inside the split bundle and readable at query time.
//! * [`QueryExtension`]: a query type evaluated by the application inside leaf search, with an
//!   asynchronous per-split warmup ([`ExtensionWarmup`]) for data tantivy does not know about.

mod query;
mod sidecar;

pub use query::{
    ExtensionQueryBuild, ExtensionWarmup, ExtensionWarmups, QueryExtension, query_extension,
    register_query_extension,
};

pub use sidecar::{
    SidecarMergeSource, SidecarWriter, SplitSidecar, register_split_sidecar, split_sidecars,
};
