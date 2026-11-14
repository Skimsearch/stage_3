package com.skimsearch.search;

import com.skimsearch.common.IndexUnavailableException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SearchApiTest {

    @TempDir
    Path dir;

    @Test
    void readyOnlyWhenServedVersionIsAtLeastPublished() throws Exception {
        Path db = Files.createFile(dir.resolve("index-v2.db"));
        var reader = new SqliteIndexReader(db, 2);
        assertTrue(new SearchApi("w1", reader, () -> 2).readiness().ready());
        assertTrue(new SearchApi("w1", reader, () -> 1).readiness().ready());
        assertFalse(new SearchApi("w1", reader, () -> 3).readiness().ready());
    }

    @Test
    void notReadyWhenPublishedVersionIsUnknown() throws Exception {
        Path db = Files.createFile(dir.resolve("index-v1.db"));
        var api = new SearchApi("w1", new SqliteIndexReader(db, 1), () -> {
            throw new IndexUnavailableException("hazelcast down");
        });
        assertFalse(api.readiness().ready());
    }

    @Test
    void notReadyWhenNothingIsLoaded() {
        var api = new SearchApi("w1", new SqliteIndexReader(dir.resolve("missing.db"), 1), () -> 1);
        var r = api.readiness();
        assertFalse(r.ready());
        assertEquals(0, r.servedVersion());
    }
}