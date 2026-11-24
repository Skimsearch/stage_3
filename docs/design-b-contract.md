# Design B contract (Indexing <-> Search)

Design B keeps the index in Hazelcast memory instead of one SQLite file per version. Only the data
location and the way Search reads it change: the two-step publication (candidate, loaded, published)
and the keys `index-meta`, `index-versions` and `search-loaded` are the same as in design A.
The design is selected at start-up with `INDEX_DESIGN=A|B` (default `A`).

## Structures of a version N

| Map (name from `HazelcastNames`) | Key | Value |
|---|---|---|
| `postings-v{N}` | term (lower case, letters only, as normalised by Search) | JSON array of `[bookId, tf]`, sorted by `tf` DESC then `bookId` ASC |
| `books-v{N}` | `bookId` (Integer) | JSON of `common.Book` |

Example of a `postings-v1` entry: key `darcy`, value `[[1,3],[7,1]]`.

## Rules

1. **Immutable once announced.** Indexing fills both maps completely, then calls `announceCandidate`.
   Nothing is written to `postings-v{N}` or `books-v{N}` after that.
2. **Search reads one pinned version.** A request takes `snapshot()` first and reads only the maps of that
   version (`IndexReader` contract, rule 7), so it never sees a half-built or a newer version.
3. **Order is stored, not computed.** The posting lists are already in the order `IndexReader` requires;
   Search applies filters (author, language, year) with `books-v{N}.getAll(ids)` and pages the result.
4. **Manifest in `index-versions`.** Design B reuses `IndexManifest` with: `file` = `postings-v{N}` (a bare
   name, as required), `sha256` = digest of the postings entries computed by the writer, `sizeBytes` = total size
   of the values, `createdAtMillis` = when the version became candidate. There is no file on disk.
5. **Loaded in B** means that the maps of N exist and are complete; a Search reports it as soon as it checks this,
   so the same promotion rule applies.
6. **Cleanup.** Versions older than N-1 may be deleted once N is published (N-1 stays so running requests finish).
7. **Backups.** Both maps are configured with at least one backup, so one member can leave without losing data.
8. **Restart.** A full restart of the Hazelcast cluster empties these maps. Recovery is a complete rebuild from
   the datalake, since there are no files to restore from.

## Open points (to agree before the writer and the reader are merged)

- Whether the Search nodes are Hazelcast members (hold partitions) or clients, as in design A.
- Value format: JSON first, a binary format only if measurements show it is needed.