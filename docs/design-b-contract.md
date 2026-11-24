# Design B contract (Indexing <-> Search)

Design B keeps the index in Hazelcast memory instead of one SQLite file per version. Only the data
location and the way Search reads it change: the two-step publication (candidate, loaded, published)
and the keys `index-meta`, `index-versions` and `search-loaded` are the same as in design A.
The design is selected at start-up with `INDEX_DESIGN=A|B` (default `A`).

## Structures of a version N

| Map (name from `HazelcastNames`) | Key | Value |
|---|---|---|
| `postings-v{N}` | term (lower case, letters only, as normalised by Search) | `int[]` with interleaved pairs `bookId, tf`, sorted by `tf` DESC then `bookId` ASC |
| `books-v{N}` | `bookId` (Integer) | JSON of `common.Book` (Gson default settings) |

- Postings: `int[]{1,3,7,1}` means book 1 with tf 3, then book 7 with tf 1. The length is always even.
  Hazelcast serialises `int[]` natively, so no codec is needed. (Measured by Sara: decoding a 1,000-entry list
  costs about 452 us as JSON and 46 us as `int[]`; JSON would bias the A/B latency comparison.)
- Books: the fields are exactly those of the record `common.Book`: `bookId`, `title`, `author`, `language`,
  `year`, `termCount`, `indexedAt`. `termCount` and `indexedAt` mean the same as the columns `term_count` and
  `indexed_at` of design A. A missing `year` (null) is left out by Gson: the reader reads it as null.

```json
{"bookId":1,"title":"Pride and Prejudice","author":"Jane Austen","language":"en","year":1998,"termCount":14,"indexedAt":"2026-11-24T10:00:00Z"}
```

## Rules

1. **Immutable once announced.** Indexing fills both maps completely, then calls `announceCandidate`.
   Nothing is written to `postings-v{N}` or `books-v{N}` after that.
2. **Search reads one pinned version.** A request takes `snapshot()` first and reads only the maps of that
   version (`IndexReader` contract, rule 7), so it never sees a half-built or a newer version.
3. **Order is stored, not computed.** The posting lists already have the order `IndexReader` requires;
   Search applies filters (author, language, year) with `books-v{N}.getAll(ids)` and pages the result.
4. **Manifest in `index-versions`.** Design B reuses `IndexManifest`: `file` = `postings-v{N}` (a bare name),
   `createdAtMillis` = when the version became candidate, and:
   - `sha256` and `sizeBytes` are computed on a canonical UTF-8 text made of, in this order, one line per term of
     `postings-v{N}` sorted by `String.compareTo` of the term, `term<TAB>v1,v2,...,vn<LF>` (the values of the
     `int[]` separated by commas, no spaces), followed by one line per book sorted by ascending `bookId`,
     `bookId<TAB>json<LF>`. The JSON is used **byte for byte as stored** in `books-v{N}` and is never
     re-serialised: Gson escapes characters such as the apostrophe (`Alice's` is stored as `Alice\u0027s`), while
     other serialisers (for example Python's `json.dumps`) would write `'`, which gives a different `sha256` for an
     identical book. A checker must read the stored string, not rebuild it.
   - `sha256` is the lower-case hex SHA-256 of that text and `sizeBytes` its length in bytes, so an integrity
     check can recompute both from the maps.
5. **Loaded in B** means that the manifest of N exists in `index-versions`. Indexing writes it after both maps are
   complete, so its existence already means complete. A Search reports N in `search-loaded` once it sees it, so
   the same promotion rule as in design A applies.
6. **Cleanup.** Versions older than N-1 may be deleted once N is published (N-1 stays so running requests finish).
   Deleting a version means destroying its two maps and removing its entry in `index-versions`.
7. **Backups.** Both maps are configured with at least one backup, so one member can leave without losing data.
8. **Full restart keeps the numbering.** A restart of the whole Hazelcast cluster empties these maps, and a
   Search never lowers the published version it has seen. Indexing therefore keeps `published-version.txt` in the
   shared folder, as in design A, and after a full restart rebuilds the index from the datalake as version
   `max(published-version.txt, versions left in index-versions) + 1`, never from v1 (after a full restart
   `index-versions` is usually empty, so in practice the number comes from the file). Until that version is
   published, Search is not ready (the old maps no longer exist).
   **Write order:** `published-version.txt` is written *before* `index.publishedVersion` is raised. If the process
   dies in between, the file is ahead of what Searches saw, and the worst case after a restart is a skipped version
   number, which is harmless. The opposite order could leave the file behind: the rebuild would publish the same
   number again, possibly with different content (the datalake may have grown), so one number would mean two
   contents.
9. **Memory.** While a switch is in progress, versions N and N-1 coexist and each has a backup, so up to four
   times the size of one copy of the index is held in the cluster. This is part of the resource-use measurements.

## Decisions taken

- The Search nodes are Hazelcast members (the worker program already starts a member), so they hold partitions.
  The cost is rebalancing when a Search joins or leaves, which is measured in design B.
- Postings as `int[]`, books as JSON.