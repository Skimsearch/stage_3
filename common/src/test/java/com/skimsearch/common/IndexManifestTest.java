package com.skimsearch.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class IndexManifestTest {

    private static final String SHA = "a".repeat(64);

    @Test
    void fileNamesFollowTheAgreedPattern() {
        assertEquals("index-v5.db", IndexManifest.dbFileName(5));
        assertEquals("index-v5.manifest.json", IndexManifest.manifestFileName(5));
    }

    @Test
    void acceptsAValidManifest() {
        var m = new IndexManifest(5, "index-v5.db", SHA, 1024, 1_731_600_000_000L);
        assertEquals(5, m.version());
    }

    @Test
    void rejectsPathsBecauseEachNodeMountsTheSharedFolderElsewhere() {
        assertThrows(IllegalArgumentException.class,
                () -> new IndexManifest(5, "Z:\\shared\\index-v5.db", SHA, 1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new IndexManifest(5, "/mnt/shared/index-v5.db", SHA, 1, 0));
    }

    @Test
    void rejectsInvalidVersionAndChecksum() {
        assertThrows(IllegalArgumentException.class,
                () -> new IndexManifest(0, "index-v0.db", SHA, 1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new IndexManifest(5, "index-v5.db", "ABC", 1, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new IndexManifest(5, "index-v5.db", SHA.toUpperCase(), 1, 0));
    }
}