package com.skimsearch.common;

import java.util.List;
import java.util.Optional;

/**
 * Read side of the index, shared by design A (local SQLite copy) and design B (Hazelcast).
 *
 * Contract:
 *  1. Results ordered by tf DESC, then bookId ASC.
 *  2. term is already normalised by Search (lower case, letters only).
 *  3. Shared types: SearchResult, Book, Filters. Author and language filters
 *     compare with Filters.sameText (case-insensitive for A-Z only, like SQLite LOWER).
 *  4. servedVersion() is the integer version this reader answers with (0 = nothing loaded).
 *     Published versions start at 1 and only go up.
 *  5. Readiness: Search is ready only if servedVersion() > 0 and >= the published version.
 *  6. If the index is unavailable, methods throw IndexUnavailableException,
 *     never an empty result.
 */
public interface IndexReader {

    List<SearchResult> search(String term, Filters filters, int offset, int limit);

    int count(String term, Filters filters);

    Optional<Book> findBook(int bookId);

    long servedVersion();
}