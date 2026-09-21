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

//! A query extension that filters documents on their sidecar row, run through a real
//! single-node search: the sidecar is shipped in the split, opened through the split's
//! directory, fetched asynchronously by the extension warmup, and read synchronously by the
//! query. Its own process because the extension registries are process-global.

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use quickwit_extensions::{
    ExtensionQueryBuild, ExtensionWarmup, QueryExtension, SidecarMergeSource, SidecarWriter,
    SplitSidecar, register_query_extension, register_split_sidecar,
};
use quickwit_indexing::TestSandbox;
use quickwit_proto::search::SearchRequest;
use quickwit_query::query_ast::{ExtensionQuery, QueryAst};
use quickwit_search::single_node_search;
use serde_json::{Map, Value as JsonValue, json};
use tantivy::query::{EnableScoring, Explanation, Query, Scorer, Weight};
use tantivy::schema::Schema;
use tantivy::{Directory, DocId, DocSet, Score, Searcher, SegmentReader, TERMINATED};

const FILE: &str = "tag.rows";

struct TagSidecar;

struct TagWriter(Vec<String>);

impl SplitSidecar for TagSidecar {
    fn file_name(&self) -> &str {
        FILE
    }
    fn extract(&self, doc: &mut Map<String, JsonValue>) -> Option<Bytes> {
        match doc.remove("_tag")? {
            JsonValue::String(tag) => Some(Bytes::from(tag)),
            _ => None,
        }
    }
    fn new_writer(&self) -> io::Result<Box<dyn SidecarWriter>> {
        Ok(Box::new(TagWriter(Vec::new())))
    }
    fn merge(&self, _: &[SidecarMergeSource], _: &Path) -> io::Result<()> {
        unimplemented!("not exercised")
    }
    fn hotcache_ranges(&self, file: &[u8]) -> Vec<std::ops::Range<usize>> {
        // Keep the first byte hot, to check the ranges reach the hotcache.
        vec![0..file.len().min(1)]
    }
}

impl SidecarWriter for TagWriter {
    fn push(&mut self, payload: Option<Bytes>) -> io::Result<()> {
        self.0
            .push(payload.map_or(String::new(), |b| String::from_utf8(b.to_vec()).unwrap()));
        Ok(())
    }
    fn num_rows(&self) -> u32 {
        self.0.len() as u32
    }
    fn finish(self: Box<Self>, out: &Path) -> io::Result<()> {
        std::fs::write(out, self.0.join("\n"))
    }
}

/// Rows loaded by the warmup, shared with the query built for the same split.
type Rows = Arc<Mutex<Option<Vec<String>>>>;

struct TagExtension;

impl QueryExtension for TagExtension {
    fn kind(&self) -> &str {
        "tag"
    }
    fn build(&self, payload: &JsonValue, _: &Schema) -> Result<ExtensionQueryBuild, String> {
        let tag = payload["tag"].as_str().ok_or("missing tag")?.to_string();
        let rows: Rows = Arc::default();
        Ok(ExtensionQueryBuild {
            query: Box::new(TagQuery {
                tag,
                rows: rows.clone(),
            }),
            warmup: Some(Arc::new(TagWarmup { rows })),
            required_terms: Vec::new(),
        })
    }
}

struct TagWarmup {
    rows: Rows,
}

#[async_trait]
impl ExtensionWarmup for TagWarmup {
    async fn warm(&self, _: &Searcher, split_directory: &dyn Directory) -> anyhow::Result<()> {
        let file = split_directory.open_read(Path::new(FILE))?;
        let bytes = file.read_bytes_async().await?;
        let text = String::from_utf8(bytes.as_slice().to_vec())?;
        *self.rows.lock().unwrap() = Some(text.split('\n').map(str::to_string).collect());
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct TagQuery {
    tag: String,
    rows: Rows,
}

impl Query for TagQuery {
    fn weight(&self, _: EnableScoring<'_>) -> tantivy::Result<Box<dyn Weight>> {
        Ok(Box::new(self.clone()))
    }
}

impl Weight for TagQuery {
    fn scorer(&self, reader: &SegmentReader, _: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let rows = self.rows.lock().unwrap();
        let rows = rows.as_ref().expect("warmup ran before the query");
        let docs: Vec<DocId> = (0..reader.max_doc())
            .filter(|&doc| rows[doc as usize] == self.tag)
            .collect();
        Ok(Box::new(Docs { docs, cursor: 0 }))
    }
    fn explain(&self, _: &SegmentReader, _: DocId) -> tantivy::Result<Explanation> {
        Ok(Explanation::new("tag", 1.0))
    }
}

struct Docs {
    docs: Vec<DocId>,
    cursor: usize,
}

impl DocSet for Docs {
    fn advance(&mut self) -> DocId {
        self.cursor += 1;
        self.doc()
    }
    fn doc(&self) -> DocId {
        self.docs.get(self.cursor).copied().unwrap_or(TERMINATED)
    }
    fn size_hint(&self) -> u32 {
        self.docs.len() as u32
    }
}

impl Scorer for Docs {
    fn score(&mut self) -> Score {
        1.0
    }
}

#[tokio::test]
async fn test_extension_query_filters_on_sidecar_rows_inside_leaf_search() -> anyhow::Result<()> {
    quickwit_common::setup_logging_for_tests();
    register_split_sidecar(Arc::new(TagSidecar));
    register_query_extension(Arc::new(TagExtension));
    let doc_mapping_yaml = r#"
        mode: strict
        field_mappings:
          - name: body
            type: text
    "#;
    let sandbox = TestSandbox::create("ext-index", doc_mapping_yaml, "{}", &["body"]).await?;
    sandbox
        .add_documents(vec![
            json!({"body": "a", "_tag": "red"}),
            json!({"body": "b", "_tag": "blue"}),
            json!({"body": "c", "_tag": "red"}),
            json!({"body": "d"}),
        ])
        .await?;
    let search = |query_ast: QueryAst, max_hits: u64| SearchRequest {
        index_id_patterns: vec!["ext-index".to_string()],
        query_ast: serde_json::to_string(&query_ast).unwrap(),
        max_hits,
        ..Default::default()
    };
    let tag = |t: &str| {
        QueryAst::Extension(ExtensionQuery {
            kind: "tag".to_string(),
            payload: json!({"tag": t}),
        })
    };

    let response = single_node_search(
        search(tag("red"), 10),
        sandbox.metastore(),
        sandbox.storage_resolver(),
    )
    .await?;
    assert!(response.failed_splits.is_empty(), "{:?}", response.failed_splits);
    assert_eq!(response.num_hits, 2, "counted inside the leaf");
    let mut bodies: Vec<String> = response
        .hits
        .iter()
        .map(|hit| serde_json::from_str::<JsonValue>(&hit.json).unwrap()["body"].to_string())
        .collect();
    bodies.sort();
    assert_eq!(bodies, ["\"a\"", "\"c\""]);

    // Rejected documents never reach the collector: paging is exact.
    let response = single_node_search(
        search(tag("red"), 1),
        sandbox.metastore(),
        sandbox.storage_resolver(),
    )
    .await?;
    assert_eq!((response.num_hits, response.hits.len()), (2, 1));

    let response = single_node_search(
        search(tag("green"), 10),
        sandbox.metastore(),
        sandbox.storage_resolver(),
    )
    .await?;
    assert_eq!(response.num_hits, 0);

    // An unknown kind is rejected, not silently ignored.
    let unknown = QueryAst::Extension(ExtensionQuery {
        kind: "nope".to_string(),
        payload: JsonValue::Null,
    });
    let error = single_node_search(
        search(unknown, 10),
        sandbox.metastore(),
        sandbox.storage_resolver(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("unknown query extension"), "{error}");

    sandbox.assert_quit().await;
    Ok(())
}
