package com.skimsearch.search;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.skimsearch.common.Filters;
import com.skimsearch.common.IndexReader;
import com.skimsearch.common.IndexUnavailableException;
import com.skimsearch.common.SearchResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.skimsearch.search.DesignBFixture.Posting;
import static com.skimsearch.search.DesignBFixture.book;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HazelcastIndexReaderTest {

    HazelcastInstance hz;
    final AtomicLong published = new AtomicLong(1);
    HazelcastIndexReader reader;

    @BeforeEach
    void setUp() {
        hz = DesignBFixture.member();
        DesignBFixture.writeVersion(hz, 1,
                List.of(book(10, "Ann", "en", 2000), book(20, "Bob", "en", 2001),
                        book(30, "Ann", "es", 2000), book(40, "Émile Zola", "fr", null)),
                List.of(new Posting("love", 30, 7), new Posting("love", 20, 9),
                        new Posting("love", 10, 7), new Posting("love", 40, 1),
                        new Posting("war", 20, 2)));
        reader = new HazelcastIndexReader(hz, published::get, Duration.ofHours(1));
    }

    @AfterEach
    void tearDown() {
        reader.close();
        hz.shutdown();
    }

    private static List<Integer> ids(List<SearchResult> r) {
        return r.stream().map(SearchResult::bookId).toList();
    }

    @Test
    void contractOrderTotalAndPaging() {
        IndexReader v = reader.snapshot();
        assertEquals(1, v.servedVersion());
        assertEquals(4, v.count("love", Filters.none()));
        assertEquals(List.of(20, 10, 30, 40), ids(v.search("love", Filters.none(), 0, 10)));
        assertEquals(List.of(10, 30), ids(v.search("love", Filters.none(), 1, 2)));
        assertEquals(List.of(), v.search("love", Filters.none(), 10, 5));
        SearchResult first = v.search("love", Filters.none(), 0, 1).get(0);
        assertEquals(new SearchResult(20, "Title 20", "Bob", "en", 2001, 9), first);
    }

    @Test
    void filtersFollowFiltersSameText() {
        IndexReader v = reader.snapshot();
        assertEquals(List.of(10, 30), ids(v.search("love", new Filters("ANN", null, 2000), 0, 10)));
        assertEquals(2, v.count("love", new Filters("ann", null, null)));
        assertEquals(List.of(30), ids(v.search("love", new Filters(null, "ES", null), 0, 10)));
        // like SQLite LOWER(): É is not folded, so only the exact accented capital matches
        assertEquals(1, v.count("love", new Filters("ÉMILE ZOLA", null, null)));
        assertEquals(0, v.count("love", new Filters("émile zola", null, null)));
    }

    @Test
    void bookWithoutYear() {
        IndexReader v = reader.snapshot();
        assertNull(v.findBook(40).orElseThrow().year());
        assertEquals(0, v.count("love", new Filters(null, null, 1999)));
        assertTrue(v.findBook(99).isEmpty());
    }

    @Test
    void absentTermIsZeroResultsWhileTheVersionExists() {
        assertEquals(0, reader.snapshot().count("whale", Filters.none()));
        assertEquals(List.of(), reader.snapshot().search("whale", Filters.none(), 0, 10));
    }

    @Test
    void lostMapsGiveUnavailableNeverAnEmptyList() {
        IndexReader v = reader.snapshot();
        assertEquals(4, v.count("love", Filters.none()));
        hz.getDistributedObjects().forEach(DistributedObject::destroy);   // like a full Hazelcast restart
        assertThrows(IndexUnavailableException.class, () -> reader.snapshot().count("love", Filters.none()));
        assertEquals(0, reader.servedVersion(), "not ready: the published version no longer exists");
    }

    @Test
    void unknownPublishedVersionServesNothing() {
        var noPublished = new HazelcastIndexReader(hz, () -> {
            throw new IndexUnavailableException("hazelcast down");
        }, Duration.ofHours(1));
        assertEquals(0, noPublished.servedVersion());
        assertThrows(IndexUnavailableException.class, () -> noPublished.search("love", Filters.none(), 0, 10));
        noPublished.close();
    }

    @Test
    void snapshotStaysOnItsVersionWhenANewOneIsPublished() {
        IndexReader pinned = reader.snapshot();
        DesignBFixture.writeVersion(hz, 2, List.of(book(50, "Cy", "en", 2002)),
                List.of(new Posting("love", 50, 99)));
        published.set(2);
        assertEquals(4, pinned.count("love", Filters.none()), "pinned request still on v1");
        assertEquals(1, reader.snapshot().count("love", Filters.none()), "new requests on v2");
        assertEquals(2, reader.snapshot().servedVersion());
    }

    @Test
    void justPublishedVersionIsFoundBeforeTheNextRefresh() {
        DesignBFixture.writeVersion(hz, 3, List.of(book(60, "Di", "en", 2003)), List.of(new Posting("sea", 60, 1)));
        published.set(3);   // the refresher runs every hour in this test
        assertEquals(3, reader.servedVersion());
        assertEquals(3, reader.highestAvailableVersion());
    }

    @Test
    void notPublishedVersionIsNotServed() {
        published.set(5);   // published key ahead of the maps (e.g. right after a restart)
        assertEquals(0, reader.servedVersion());
        assertThrows(IndexUnavailableException.class, () -> reader.search("love", Filters.none(), 0, 10));
    }
}