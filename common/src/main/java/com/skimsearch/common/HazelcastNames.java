package com.skimsearch.common;

/**
 * Names of the Hazelcast structures shared by Indexing and Search.
 * Plain strings, so common stays free of the Hazelcast dependency.
 * All version values are Long.
 */
public final class HazelcastNames {

    /** IMap String -> Long with the index version keys below. */
    public static final String META_MAP = "index-meta";
    public static final String PUBLISHED_KEY = "index.publishedVersion";
    public static final String CANDIDATE_KEY = "index.candidateVersion";

    /** IMap Long version -> manifest JSON. */
    public static final String VERSIONS_MAP = "index-versions";

    /** IMap String workerId -> Long highest loaded version (TTL, one entry per active Search). */
    public static final String LOADED_MAP = "search-loaded";

    // ---- Design B: the index lives in Hazelcast memory, one pair of maps per version ----

    /**
     * IMap String term -> JSON array of [bookId, tf] pairs, already sorted by tf DESC then
     * bookId ASC (the order required by IndexReader). The term is normalised as in
     * Search (lower case, letters only). One entry per term of the version.
     */
    private static final String POSTINGS_PREFIX = "postings-v";

    /** IMap Integer bookId -> JSON of common.Book (title, author, language, year, ...). */
    private static final String BOOKS_PREFIX = "books-v";

    /** Name of the postings map of a version (design B), e.g. postings-v3. */
    public static String postingsMap(long version) {
        return POSTINGS_PREFIX + checkVersion(version);
    }

    /** Name of the books map of a version (design B), e.g. books-v3. */
    public static String booksMap(long version) {
        return BOOKS_PREFIX + checkVersion(version);
    }

    private static long checkVersion(long version) {
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1: " + version);
        }
        return version;
    }

    private HazelcastNames() {
    }
}