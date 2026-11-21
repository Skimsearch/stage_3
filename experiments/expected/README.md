# Expected Search Results

This folder contains the expected Search results used by the Stage 3 load
generator to validate query correctness.

Files follow this naming convention:

experiments/expected/version-{N}.json

Example:

{
  "love": [1342, 84],
  "alice": [11],
  "pride": [1342]
}

Each key is a query from experiments/queries.txt.

Each value is the list of book IDs expected for that query in a specific logical
index version.

The expected results must correspond to the same logical index state used in
both compared Stage 3 designs.

The load generator receives the expected-results file with:

--expected experiments/expected/version-1.json

If --expected is omitted, the load generator can still be used for development
or connectivity checks, but HTTP success alone must not be interpreted as proof
of query correctness in the final experiments.

Final expected-results files should be generated or validated by the Search
workload owner using the same corpus and index version used in the paired runs.