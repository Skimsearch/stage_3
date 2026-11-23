# Expected Search Results

This folder contains the logical expected Search results used by the Stage 3
load generator to validate query correctness.

Expected results describe the logical index content and do not depend on the
runtime index version number.

Recommended file:

experiments/expected/search-results.json

Preferred format:

{
  "whale": {
    "total": 3,
    "top": [84, 2701, 1513]
  },
  "alice": {
    "total": 1,
    "top": [11]
  }
}

For each query:

- total is the total number of matching books.
- top is the ordered list of book IDs returned by Search.
- Order matters and must follow the Search contract.
- The load generator sends limit=100.

Both distributed designs must use the same corpus, tokenizer, query set and
logical expected results.

Runtime index versions may differ between designs or runs because publication,
batching and flush timing can produce different version numbers even when the
logical index content is equivalent.

For this reason, the index version returned by Search is recorded as experiment
metadata but is not used to select the expected result set.

Run the load generator with:

--expected experiments/expected/search-results.json

The old list-only format is still accepted for compatibility:

{
  "whale": [84, 2701, 1513]
}

In that format, total is assumed to be the length of the list.

If --expected is omitted, the generator may be used for connectivity or
development checks, but HTTP success alone is not evidence of query correctness
for the final comparative experiments.

Correctness comparisons are only valid for fixed-corpus runs where both designs
use the same logical document set.