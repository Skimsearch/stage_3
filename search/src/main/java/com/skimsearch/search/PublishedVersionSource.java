package com.skimsearch.search;

/**
 * Where Search reads the latest index version published by Indexing.
 * Final implementation: Hazelcast key "index.publishedVersion".
 * Until then: a fixed value from the PUBLISHED_VERSION environment variable.
 */
@FunctionalInterface
public interface PublishedVersionSource {

    /** Throws IndexUnavailableException if the value cannot be read. */
    long publishedVersion();
}