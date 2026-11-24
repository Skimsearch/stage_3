package com.skimsearch.indexing;

import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Format of event=index-build-finished, which the indexing/s measurements parse. */
class IndexBuildRateTest {

    @Test
    void rateIsBooksDividedByBuildTime() {
        // 4 books in 0.5 s = 8 books/s; 20 ms waiting for the lock
        assertEquals("event=index-build-finished version=3 requested=4 books=4 rebuild=false buildMs=500.0 "
                        + "lockWaitMs=20.0 booksPerSecond=8.00",
                IndexingRole.buildFinishedEvent(3, 4, 4, 500_000_000L, 20_000_000L));
    }

    @Test
    void rebuildUsesTheBooksActuallyBuilt() {
        // task asked for 1 book, design B rebuilt 4 after a Hazelcast restart: 4 books in 0.5 s
        assertEquals("event=index-build-finished version=7 requested=1 books=4 rebuild=true buildMs=500.0 "
                        + "lockWaitMs=0.0 booksPerSecond=8.00",
                IndexingRole.buildFinishedEvent(7, 1, 4, 500_000_000L, 0));
    }

    @Test
    void zeroDurationGivesZeroRateInsteadOfInfinity() {
        assertEquals("event=index-build-finished version=1 requested=2 books=2 rebuild=false buildMs=0.0 "
                        + "lockWaitMs=0.0 booksPerSecond=0.00",
                IndexingRole.buildFinishedEvent(1, 2, 2, 0, 0));
    }

    @Test
    void decimalSeparatorIsAlwaysAPoint() {
        Locale before = Locale.getDefault();
        Locale.setDefault(new Locale("es", "ES"));
        try {
            assertEquals("event=index-build-finished version=2 requested=1 books=1 rebuild=false buildMs=250.0 "
                            + "lockWaitMs=0.5 booksPerSecond=4.00",
                    IndexingRole.buildFinishedEvent(2, 1, 1, 250_000_000L, 500_000L));
        } finally {
            Locale.setDefault(before);
        }
    }
}
