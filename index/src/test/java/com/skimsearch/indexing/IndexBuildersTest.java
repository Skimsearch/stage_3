package com.skimsearch.indexing;

import com.hazelcast.config.Config;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** INDEX_DESIGN selection and the map settings of design B; no Hazelcast member is started. */
class IndexBuildersTest {

    private static final DatalakeReader LAKE = new DatalakeReader(Path.of("unused"));

    @Test
    void designAIsTheDefault() {
        assertInstanceOf(VersionedIndexingService.class, IndexBuilders.create(null, LAKE, Path.of("idx"), null));
        assertInstanceOf(VersionedIndexingService.class, IndexBuilders.create(" ", LAKE, Path.of("idx"), null));
        assertInstanceOf(VersionedIndexingService.class, IndexBuilders.create("a", LAKE, Path.of("idx"), null));
    }

    @Test
    void designBIsSelectedCaseInsensitively() {
        assertInstanceOf(HazelcastIndexingService.class, IndexBuilders.create("B", LAKE, Path.of("idx"), null));
        assertInstanceOf(HazelcastIndexingService.class, IndexBuilders.create("b", LAKE, Path.of("idx"), null));
    }

    @Test
    void otherValuesAreRejected() {
        assertThrows(IllegalArgumentException.class, () -> IndexBuilders.create("C", LAKE, Path.of("idx"), null));
        assertThrows(IllegalArgumentException.class, () -> IndexBuilders.checkDesign("both"));
        assertDoesNotThrow(() -> IndexBuilders.checkDesign("B"));
    }

    @Test
    void designBMapsKeepOneBackup() {
        Config config = IndexMain.hazelcastConfig(Map.of());

        assertTrue(config.getMapConfigs().containsKey("postings-v*"));
        assertTrue(config.getMapConfigs().containsKey("books-v*"));
        assertTrue(config.getMapConfig("postings-v7").getBackupCount() >= 1);
        assertTrue(config.getMapConfig("books-v7").getBackupCount() >= 1);
    }
}
