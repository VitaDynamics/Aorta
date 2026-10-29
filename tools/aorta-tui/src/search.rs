use std::sync::Arc;

use nucleo::pattern::{CaseMatching, Normalization};
use nucleo::{Config, Nucleo};

#[derive(Debug, Clone)]
struct SearchEntry {
    row_index: usize,
    haystack: String,
}

pub struct SearchIndex {
    matcher: Nucleo<SearchEntry>,
    query: String,
    all_indices: Vec<usize>,
}

impl SearchIndex {
    pub fn new() -> Self {
        Self {
            matcher: Nucleo::new(Config::DEFAULT, Arc::new(|| {}), Some(1), 1),
            query: String::new(),
            all_indices: Vec::new(),
        }
    }

    pub fn replace(&mut self, entries: impl IntoIterator<Item = (usize, String)>) {
        self.matcher.restart(true);
        self.all_indices.clear();

        let injector = self.matcher.injector();
        for (row_index, haystack) in entries {
            self.all_indices.push(row_index);
            injector.push(
                SearchEntry {
                    row_index,
                    haystack,
                },
                |entry, columns| {
                    columns[0] = entry.haystack.as_str().into();
                },
            );
        }

        let query = self.query.clone();
        self.set_query(&query);
    }

    pub fn set_query(&mut self, query: &str) {
        let append = query.starts_with(&self.query);
        self.query.clear();
        self.query.push_str(query);
        self.matcher.pattern.reparse(
            0,
            &self.query,
            CaseMatching::Ignore,
            Normalization::Smart,
            append,
        );
        self.finish_matching();
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    pub fn matched_indices(&mut self) -> Vec<usize> {
        if self.query.is_empty() {
            return self.all_indices.clone();
        }

        self.finish_matching();
        self.matcher
            .snapshot()
            .matched_items(..)
            .map(|item| item.data.row_index)
            .collect()
    }

    fn finish_matching(&mut self) {
        for _ in 0..4 {
            let status = self.matcher.tick(10);
            if !status.running {
                break;
            }
        }
    }
}

impl Default for SearchIndex {
    fn default() -> Self {
        Self::new()
    }
}
