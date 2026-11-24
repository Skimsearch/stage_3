package com.skimsearch.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void designBMapNamesAreTheAgreedOnes() {
        assertEquals("postings-v1", HazelcastNames.postingsMap(1));
        assertEquals("postings-v12", HazelcastNames.postingsMap(12));
        assertEquals("books-v1", HazelcastNames.booksMap(1));
        assertEquals("books-v12", HazelcastNames.booksMap(12));
    }

    @Test
    void designBMapNamesRejectInvalidVersions() {
        assertThrows(IllegalArgumentException.class, () -> HazelcastNames.postingsMap(0));
        assertThrows(IllegalArgumentException.class, () -> HazelcastNames.booksMap(-1));
    }
}