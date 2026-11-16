package com.skimsearch.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The agreed names: changing one breaks running clusters and old logs, so it must be deliberate. */
class HazelcastNamesTest {

    @Test
    void namesAreTheAgreedOnes() {
        assertEquals("index-meta", HazelcastNames.META_MAP);
        assertEquals("index.publishedVersion", HazelcastNames.PUBLISHED_KEY);
        assertEquals("index.candidateVersion", HazelcastNames.CANDIDATE_KEY);
        assertEquals("index-versions", HazelcastNames.VERSIONS_MAP);
        assertEquals("search-loaded", HazelcastNames.LOADED_MAP);
    }
}