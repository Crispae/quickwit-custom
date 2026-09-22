# The `rustie-ext` fork

This branch adds **generic extension points** to Quickwit, used by
[quick-rustie](https://github.com/Crispae/RustIE) to run linguistic pattern matching (token
sequences + dependency-graph traversal) *inside* leaf search. No RustIE code lives here: an
embedding application registers implementations at start-up through `quickwit-extensions`, and
nothing changes when none is registered.

Base: upstream `cc420c34` (`fix(ci): repair cross image builds (#6617)`), the commit
quick-rustie's `Cargo.lock` pins as `tag = "v0.9.0"`.

## Hooks

| Commit | Hook | Touches |
| --- | --- | --- |
| `feat(extensions): split sidecar extension point` | `SplitSidecar`: an extra per-split file, row *i* = document *i*. Payload extracted from the raw JSON in the doc processor, rows pushed in the same call as `add_document`, written by `IndexedSplitBuilder::finalize`, bundled by the packager, rebuilt by the merge executor from the alive documents of each input segment in merge order. | `quickwit-extensions` (new), `quickwit-indexing` (`doc_processor`, `indexer`, `indexed_split`, `processed_doc`, `packager`, `merge_executor`) |
| `feat(extensions): list split sidecars in the hotcache` | Sidecars are listed in the split hotcache (so the split directory can open them) and the extension's always-needed ranges are cached there. | `quickwit-directories/hot_directory.rs` |
| `feat(extensions): query extensions evaluated inside leaf search` | `QueryAst::Extension { kind, payload }` built by a registered `QueryExtension` into a tantivy query + an async `ExtensionWarmup` (run after Quickwit's warmup with the split searcher and raw split directory) + required terms. | `quickwit-query` (`query_ast/{mod,extension_query,visitor}.rs`), `quickwit-doc-mapper` (`WarmupInfo`, `query_builder`, `tag_pruning`), `quickwit-search/leaf.rs` (`warmup`) |
| `feat(extensions): tokenizers registered by name` | `register_tokenizer(name, TextAnalyzer)`: `create_default_quickwit_tokenizer_manager` adds every registered tokenizer after the built-ins (a name that collides with a built-in is dropped with a `warn!`). Every doc mapper starts from that manager, so indexing, merges, leaf search and field validation all know the name. Quickwit's config tokenizers are a closed list whose regex tokenizer always numbers positions 0, 1, 2, ...; a registered Tantivy tokenizer sets `Token::position` itself, e.g. several terms at one position. Register before building any doc mapper; the name must not appear in a mapping's `tokenizers:` section. | `quickwit-extensions/tokenizer.rs`, `quickwit-query/tokenizers/mod.rs` |

Why the query filters in its scorer rather than after collection: the collector counts
`num_hits`, keeps the top-k and honours `search_after`. Rejecting documents before it sees them
keeps all three exact and means rejected documents are never fetched.

## Fixes carried by the fork (not hooks)

| Commit | Fix | Touches |
| --- | --- | --- |
| `fix(indexing): create the vec source from its typed params, not through JSON` | `TypedSourceFactory` obtains a source's params by serializing the source config to `serde_json::Value` and back; for a vec source that turns every document byte into a JSON number (~25 µs and ~23× the batch bytes of peak memory per document, on every pipeline spawn). `VecSourceFactory` now implements `SourceFactory` directly. Upstream-worthy on its own. | `quickwit-indexing/src/source/vec_source.rs` |

## Invariants the hooks rely on

- Quickwit never sets `sort_by_field`, so a split's documents are numbered in `add_document`
  order, and a merge numbers its output by stacking the alive documents of the segments in the
  order passed to `IndexWriter::merge`. The merge executor re-checks the row count against the
  merged segment.
- `combine_index_meta` does not keep the input split order, so the merge hook maps segments to
  their input directories through each split's own `meta.json`.
- Tantivy queries run synchronously on data made local by warmup; extension warmups run after
  Quickwit's own, so they can read the postings it warmed.

## Tests

```bash
cargo test -p quickwit-indexing --test split_sidecar     # rows follow doc ids: index, merge, delete+merge
cargo test -p quickwit-search --test query_extension     # sidecar-driven extension query in a real search
cargo test -p quickwit-doc-mapper --lib test_extension_tokenizer   # registered tokenizer: known name, own positions
```

Known upstream flakiness at the base commit (not caused by the fork):
`quickwit-indexing` `test_indexing_service_shutdown_merge_pipeline_when_no_indexing_pipeline`
(intermittent) and `quickwit-doc-mapper` `test_concatenate_multiple_field` (always).

## Rebasing on upstream

1. `git fetch upstream && git rebase upstream/<tag>`; conflicts are confined to the files above.
2. Re-run the tests above.
3. Update quick-rustie's `Cargo.lock` / `[patch]` to the new base.
