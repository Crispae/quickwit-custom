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

use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use quickwit_extensions::ExtensionWarmup;
use serde::{Deserialize, Serialize};
use tantivy::Term;

use crate::InvalidQuery;
use crate::query_ast::tantivy_query_ast::TantivyQueryAst;
use crate::query_ast::{BuildTantivyAst, BuildTantivyAstContext, QueryAst};

/// A query evaluated by an application-registered [`quickwit_extensions::QueryExtension`].
///
/// Quickwit treats it as an opaque leaf: it builds it through the registry, warms up the terms
/// it reports, runs the extension's warmup, then executes it like any tantivy query. A node on
/// which no extension of this `kind` is registered rejects the query.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionQuery {
    pub kind: String,
    #[serde(default)]
    pub payload: serde_json::Value,
}

impl From<ExtensionQuery> for QueryAst {
    fn from(extension_query: ExtensionQuery) -> Self {
        QueryAst::Extension(extension_query)
    }
}

/// Side outputs of extension queries collected while building one tantivy query.
#[derive(Default)]
pub struct ExtensionOutputs {
    warmups: Mutex<Vec<Arc<dyn ExtensionWarmup>>>,
    required_terms: Mutex<Vec<Term>>,
}

impl ExtensionOutputs {
    /// Warmups to run before the query executes.
    pub fn take_warmups(&self) -> Vec<Arc<dyn ExtensionWarmup>> {
        std::mem::take(&mut *self.warmups.lock().expect("poisoned"))
    }

    pub(crate) fn take_required_terms(&self) -> Vec<Term> {
        std::mem::take(&mut *self.required_terms.lock().expect("poisoned"))
    }
}

impl BuildTantivyAst for ExtensionQuery {
    fn build_tantivy_ast_impl(
        &self,
        context: &BuildTantivyAstContext,
    ) -> Result<TantivyQueryAst, InvalidQuery> {
        let extension = quickwit_extensions::query_extension(&self.kind).ok_or_else(|| {
            InvalidQuery::Other(anyhow!(
                "unknown query extension `{}`: it is not registered on this node",
                self.kind
            ))
        })?;
        let build = extension
            .build(&self.payload, context.schema)
            .map_err(|error| InvalidQuery::Other(anyhow!("{} query: {error}", self.kind)))?;
        if let Some(warmup) = build.warmup {
            context
                .extension_outputs
                .warmups
                .lock()
                .expect("poisoned")
                .push(warmup);
        }
        context
            .extension_outputs
            .required_terms
            .lock()
            .expect("poisoned")
            .extend(build.required_terms);
        Ok(TantivyQueryAst::Leaf(build.query))
    }
}
