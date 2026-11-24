package com.skimsearch.indexing;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Format of event=index-build-finished, which the indexing/s measurements parse. */
class IndexBuildRateTest {

    @Test
    void rateIsBooksDividedByBuildTime() {
        // 4 books in 0.5 s = 8 books/s; 20 ms waiting for the lock
        assertEquals("event=index-build-finished version=3 books=4 buildMs=500.0 lockWaitMs=20.0 booksPerSecond=8.00",
                IndexingRole.buildFinishedEvent(3, 4, 500_000_000L, 20_000_000L));
    }

    @Test
    void zeroDurationGivesZeroRateInsteadOfInfinity() {
        assertEquals("event=index-build-finished version=1 books=2 buildMs=0.0 lockWaitMs=0.0 booksPerSecond=0.00",
                IndexingRole.buildFinishedEvent(1, 2, 0, 0));
    }

    @Test
    void decimalSeparatorIsAlwaysAPoint() {
        java.util.Locale before = java.util.Locale.getDefault();
        java.util.Locale.setDefault(new java.util.Locale("es", "ES"));
        try {
            assertEquals("event=index-build-finished version=2 books=1 buildMs=250.0 lockWaitMs=0.5 booksPerSecond=4.00",
                    IndexingRole.buildFinishedEvent(2, 1, 250_000_000L, 500_000L));
        } finally {
            java.util.Locale.setDefault(before);
        }
    }
}
