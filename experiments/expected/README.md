# Expected Search Results

This folder contains the logical expected Search results used by the Stage 3
load generator to validate query correctness.

Expected results describe the logical index content and do not depend on the
runtime index version number.

Recommended file:

experiments/expected/search-results.json

Example:

{
  "love": [1342, 84],
  "alice": [11],
  "pride": [1342]
}

Each key is a query from experiments/queries.txt.

Each value contains the expected book IDs for that query for the common corpus
used in the experiment.

Both distributed designs must use the same corpus, tokenizer, query set and
logical expected results.

Runtime index versions may differ between designs or runs because publication,
batching and flush timing can produce different version numbers even when the
logical index content is equivalent.

For this reason, the index version returned by Search is recorded as experiment
metadata but is not used to select the expected result set.

Run the load generator with:

--expected experiments/expected/search-results.json

If --expected is omitted, the generator may be used for connectivity or
development checks, but HTTP success alone is not evidence of query correctness
for the final comparative experiments.