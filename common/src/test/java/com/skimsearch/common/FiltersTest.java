package com.skimsearch.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FiltersTest {

    @Test
    void foldsAsciiLettersOnly() {
        assertTrue(Filters.sameText("JANE AUSTEN", "Jane Austen"));
        assertTrue(Filters.sameText("en", "EN"));
    }

    @Test
    void doesNotFoldAccentedLettersLikeSqlite() {
        // Same É, the rest is ASCII: matches
        assertTrue(Filters.sameText("ÉMILE ZOLA", "Émile Zola"));
        // é vs É: SQLite LOWER() leaves both unchanged, so they differ
        assertFalse(Filters.sameText("émile zola", "Émile Zola"));
        assertFalse(Filters.sameText("ÑANDÚ", "ñandú"));
    }

    @Test
    void nullNeverMatchesAndLengthMustBeEqual() {
        assertFalse(Filters.sameText(null, "x"));
        assertFalse(Filters.sameText("x", null));
        assertFalse(Filters.sameText("Ann", "Anna"));
    }

    @Test
    void matchesAppliesOnlyTheGivenFilters() {
        assertTrue(Filters.none().matches(null, null, null));
        assertTrue(new Filters("ann", null, 2000).matches("Ann", "en", 2000));
        assertFalse(new Filters("ann", null, 2000).matches("Ann", "en", 2001));
        assertFalse(new Filters(null, "es", null).matches("Ann", null, 2000));
    }
}