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

    private HazelcastNames() {
    }
}