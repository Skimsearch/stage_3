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
 *  7. A request that makes several calls (count + search) takes one snapshot() first,
 *     so all its calls see the same version even if a new one is switched in meanwhile.
 */
public interface IndexReader {

    List<SearchResult> search(String term, Filters filters, int offset, int limit);

    int count(String term, Filters filters);

    Optional<Book> findBook(int bookId);

    long servedVersion();

    /**
     * A reader pinned to the version served right now. Readers that never change
     * version (one SQLite file) can return themselves, which is the default.
     */
    default IndexReader snapshot() {
        return this;
    }
}