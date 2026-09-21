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

//! End-to-end tests of the split sidecar extension point: rows follow document ids through
//! indexing, and the file travels in the split bundle.
//!
//! This is an integration test (its own process) on purpose: the sidecar registry is
//! process-global, and registering a sidecar inside the crate's unit-test binary would leak it
//! into every other test that builds splits by hand.

use std::io;
use std::path::Path;
use std::sync::{Arc, Once};

use bytes::Bytes;
use quickwit_actors::Universe;
use quickwit_common::io::IoControls;
use quickwit_common::split_file;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_extensions::{
    SidecarMergeSource, SidecarWriter, SplitSidecar, register_split_sidecar,
};
use quickwit_indexing::actors::MergeExecutor;
use quickwit_indexing::get_tantivy_directory_from_split_bundle;
use quickwit_indexing::merge_policy::{MergeOperation, MergeSource, MergeTask};
use quickwit_indexing::models::{IndexedSplitBatch, MergeScratch};
use quickwit_metastore::{
    ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata, StageSplitsRequestExt,
};
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::{
    DeleteQuery, ListSplitsRequest, MetastoreService, PublishSplitsRequest, StageSplitsRequest,
};
use quickwit_proto::types::SplitId;
use serde_json::{Map, Value};
use tantivy::directory::FileSlice;
use tantivy::schema::Value as _;
use tantivy::{Directory, ReloadPolicy, TantivyDocument};

use quickwit_indexing::TestSandbox;

const SIDECAR_FILE: &str = "test.rows";
const SIDECAR_FIELD: &str = "_side";

/// Rows are stored as lines: `-` for an empty row, else the payload.
struct TestSidecar;

struct TestWriter {
    rows: Vec<Option<Bytes>>,
}

impl SplitSidecar for TestSidecar {
    fn file_name(&self) -> &str {
        SIDECAR_FILE
    }

    fn extract(&self, doc: &mut Map<String, Value>) -> Option<Bytes> {
        match doc.remove(SIDECAR_FIELD)? {
            Value::String(s) => Some(Bytes::from(s)),
            _ => None,
        }
    }

    fn new_writer(&self) -> io::Result<Box<dyn SidecarWriter>> {
        Ok(Box::new(TestWriter { rows: Vec::new() }))
    }

    fn merge(&self, sources: &[SidecarMergeSource], out: &Path) -> io::Result<()> {
        let mut lines: Vec<String> = Vec::new();
        for source in sources {
            let rows: Vec<String> = match &source.data {
                Some(data) => std::str::from_utf8((**data).as_ref())
                    .unwrap()
                    .lines()
                    .map(str::to_string)
                    .collect(),
                None => vec!["-".to_string(); source.num_docs as usize],
            };
            assert_eq!(rows.len(), source.num_docs as usize);
            match &source.alive_docs {
                Some(alive) => lines.extend(alive.iter().map(|&d| rows[d as usize].clone())),
                None => lines.extend(rows),
            }
        }
        std::fs::write(out, lines.join("\n"))
    }
}

impl SidecarWriter for TestWriter {
    fn push(&mut self, payload: Option<Bytes>) -> io::Result<()> {
        self.rows.push(payload);
        Ok(())
    }

    fn num_rows(&self) -> u32 {
        self.rows.len() as u32
    }

    fn finish(self: Box<Self>, out: &Path) -> io::Result<()> {
        let lines: Vec<String> = self
            .rows
            .iter()
            .map(|row| match row {
                Some(bytes) => String::from_utf8(bytes.to_vec()).unwrap(),
                None => "-".to_string(),
            })
            .collect();
        std::fs::write(out, lines.join("\n"))
    }
}

fn register_test_sidecar() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| register_split_sidecar(Arc::new(TestSidecar)));
}

/// Rows of `SIDECAR_FILE` in every published split of the sandbox's index.
async fn published_sidecar_rows(sandbox: &TestSandbox) -> anyhow::Result<Vec<Vec<String>>> {
    let splits = sandbox
        .metastore()
        .list_splits(ListSplitsRequest::try_from_index_uid(sandbox.index_uid())?)
        .await?
        .collect_splits()
        .await?;
    let mut out = Vec::new();
    for split in splits {
        let split_id = split.split_metadata.split_id;
        let data = sandbox
            .storage()
            .get_all(Path::new(&format!("{split_id}.split")))
            .await?;
        let directory =
            quickwit_directories::BundleDirectory::open_split(FileSlice::from(data.to_vec()))?;
        let file = directory.open_read(Path::new(SIDECAR_FILE))?;
        let text = String::from_utf8(file.read_bytes()?.to_vec())?;
        out.push(text.lines().map(str::to_string).collect());
    }
    Ok(out)
}

#[tokio::test]
async fn test_sidecar_rows_follow_document_ids_and_ship_in_the_bundle() -> anyhow::Result<()> {
    quickwit_common::setup_logging_for_tests();
    register_test_sidecar();
    // The mapping does not declare `_side`: the extension must take it out first.
    let doc_mapping_yaml = r#"
        mode: strict
        field_mappings:
          - name: body
            type: text
    "#;
    let sandbox = TestSandbox::create("sidecar-index", doc_mapping_yaml, "{}", &["body"]).await?;
    sandbox
        .add_documents(vec![
            serde_json::json!({"body": "zero", "_side": "s0"}),
            serde_json::json!({"body": "one"}),
            serde_json::json!({"body": "two", "_side": "s2"}),
        ])
        .await?;
    let rows = published_sidecar_rows(&sandbox).await?;
    assert_eq!(
        rows,
        vec![vec!["s0".to_string(), "-".to_string(), "s2".to_string()]]
    );
    sandbox.assert_quit().await;
    Ok(())
}

const MAPPING: &str = r#"
    mode: strict
    field_mappings:
      - name: body
        type: text
        tokenizer: raw
"#;

fn doc(i: usize) -> serde_json::Value {
    // Every other document has no sidecar payload: holes must survive merges too.
    if i % 2 == 0 {
        serde_json::json!({"body": format!("d{i}"), "_side": format!("d{i}")})
    } else {
        serde_json::json!({"body": format!("d{i}")})
    }
}

/// The row a document should have in the sidecar.
fn expected_row(body: &str) -> String {
    let i: usize = body[1..].parse().unwrap();
    if i % 2 == 0 {
        body.to_string()
    } else {
        "-".to_string()
    }
}

/// Run the merge executor on `splits` and return the packaged split.
async fn run_merge(
    sandbox: &TestSandbox,
    universe: &Universe,
    splits: Vec<SplitMetadata>,
    operation: MergeOperation,
    source_split_ids: &[SplitId],
) -> anyhow::Result<IndexedSplitBatch> {
    let merge_scratch_directory = TempDirectory::for_test();
    let downloaded = merge_scratch_directory.named_temp_child("downloaded-splits-")?;
    let mut tantivy_dirs: Vec<Box<dyn Directory>> = Vec::new();
    for (split, source_id) in splits.iter().zip(source_split_ids) {
        let dest = downloaded.path().join(split_file(split.split_id()));
        sandbox
            .storage()
            .copy_to_file(Path::new(&split_file(source_id)), &dest)
            .await?;
        tantivy_dirs.push(get_tantivy_directory_from_split_bundle(&dest)?);
    }
    let scratch = MergeScratch {
        merge_source: MergeSource::Task(MergeTask::from_merge_operation_for_test(operation)),
        tantivy_dirs,
        merge_scratch_directory,
        downloaded_splits_directory: downloaded,
    };
    let (packager_mailbox, packager_inbox) = universe.create_test_mailbox();
    let executor = MergeExecutor::new(
        MergePipelineId {
            node_id: sandbox.node_id(),
            index_uid: sandbox.index_uid(),
            source_id: sandbox.source_id(),
        },
        sandbox.metastore(),
        sandbox.doc_mapper(),
        IoControls::default(),
        packager_mailbox,
        None,
    );
    let (mailbox, handle) = universe.spawn_builder().spawn(executor);
    mailbox.send_message(scratch).await?;
    handle.process_pending_and_observe().await;
    let mut batches: Vec<IndexedSplitBatch> = packager_inbox.drain_for_test_typed();
    assert_eq!(batches.len(), 1);
    Ok(batches.remove(0))
}

/// Check that row `i` of the merged sidecar belongs to merged document `i`.
fn assert_rows_follow_documents(batch: &IndexedSplitBatch) -> anyhow::Result<usize> {
    let split = &batch.splits[0];
    let rows = std::fs::read_to_string(split.split_scratch_directory.path().join(SIDECAR_FILE))?;
    let rows: Vec<&str> = rows.lines().collect();
    let reader = split
        .index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    assert_eq!(searcher.segment_readers().len(), 1);
    let segment = &searcher.segment_readers()[0];
    assert!(
        segment.alive_bitset().is_none(),
        "merged segment has no deletes"
    );
    let body = searcher.schema().get_field("body")?;
    assert_eq!(rows.len(), segment.max_doc() as usize);
    for doc_id in 0..segment.max_doc() {
        let doc: TantivyDocument = searcher.doc(tantivy::DocAddress::new(0, doc_id))?;
        let text = doc.get_first(body).and_then(|v| v.as_str()).unwrap();
        assert_eq!(rows[doc_id as usize], expected_row(text), "doc {doc_id}");
    }
    Ok(rows.len())
}

#[tokio::test]
async fn test_merge_stacks_sidecar_rows_in_segment_order() -> anyhow::Result<()> {
    register_test_sidecar();
    let sandbox = TestSandbox::create("sidecar-merge", MAPPING, "{}", &["body"]).await?;
    // Three splits of different sizes: any mix-up of source order shows up as misalignment.
    let mut next = 0;
    for size in [3, 5, 2] {
        sandbox
            .add_documents((next..next + size).map(doc).collect::<Vec<_>>())
            .await?;
        next += size;
    }
    let splits: Vec<SplitMetadata> = sandbox
        .metastore()
        .list_splits(ListSplitsRequest::try_from_index_uid(sandbox.index_uid())?)
        .await?
        .collect_splits_metadata()
        .await?;
    assert_eq!(splits.len(), 3);
    let ids: Vec<SplitId> = splits.iter().map(|s| s.split_id.clone()).collect();
    let universe = Universe::with_accelerated_time();
    let batch = run_merge(
        &sandbox,
        &universe,
        splits.clone(),
        MergeOperation::new_merge_operation(splits),
        &ids,
    )
    .await?;
    assert_eq!(assert_rows_follow_documents(&batch)?, 10);
    universe.assert_quit().await;
    sandbox.assert_quit().await;
    Ok(())
}

#[tokio::test]
async fn test_delete_and_merge_drops_rows_of_deleted_documents() -> anyhow::Result<()> {
    register_test_sidecar();
    let sandbox = TestSandbox::create("sidecar-delete", MAPPING, "{}", &["body"]).await?;
    sandbox
        .add_documents((0..8).map(doc).collect::<Vec<_>>())
        .await?;
    let metastore = sandbox.metastore();
    let index_uid = sandbox.index_uid();
    metastore
        .create_delete_task(DeleteQuery {
            index_uid: Some(index_uid.clone()),
            start_timestamp: None,
            end_timestamp: None,
            query_ast: quickwit_query::query_ast::qast_json_helper(
                "body:d0 OR body:d3 OR body:d4 OR body:d7",
                &["body"],
            ),
        })
        .await?;
    let splits = metastore
        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone())?)
        .await?
        .collect_splits()
        .await?;
    let source_id = splits[0].split_metadata.split_id.clone();
    // Delete-and-merge runs on a split that already went through a merge.
    let mut split = splits[0].split_metadata.clone();
    split.split_id = SplitId::new();
    split.num_merge_ops = 1;
    metastore
        .stage_splits(StageSplitsRequest::try_from_split_metadata(
            index_uid.clone(),
            &split,
        )?)
        .await?;
    metastore
        .publish_splits(PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: vec![split.split_id.to_string()],
            replaced_split_ids: vec![source_id.to_string()],
            index_checkpoint_delta_json_opt: None,
            publish_token_opt: None,
        })
        .await?;
    let universe = Universe::with_accelerated_time();
    let batch = run_merge(
        &sandbox,
        &universe,
        vec![split.clone()],
        MergeOperation::new_delete_and_merge_operation(split),
        &[source_id],
    )
    .await?;
    assert_eq!(
        assert_rows_follow_documents(&batch)?,
        4,
        "d1 d2 d5 d6 remain"
    );
    universe.assert_quit().await;
    sandbox.assert_quit().await;
    Ok(())
}
