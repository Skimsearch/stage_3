package com.skimsearch.indexing;

import com.skimsearch.common.IndexManifest;

import java.util.List;

/**
 * Builds the next index version from datalake books and returns its manifest.
 * Design A writes a SQLite file ({@link VersionedIndexingService}); design B fills
 * Hazelcast maps ({@link HazelcastIndexingService}). The caller announces the version
 * as candidate once this method has returned.
 */
public interface IndexBuilder {

    /** New version = previous version + these books. */
    IndexManifest indexBooks(List<Integer> bookIds);
}
