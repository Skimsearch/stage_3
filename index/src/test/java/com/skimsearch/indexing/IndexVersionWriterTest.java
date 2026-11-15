package com.skimsearch.indexing;

import com.skimsearch.common.IndexManifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class IndexVersionWriterTest {

    @TempDir
    Path dir;

    private static IndexStore.BookIndexData book(int id, Map<String, Integer> tf) {
        return new IndexStore.BookIndexData(
                new BookMetadata(id, "Title " + id, "Author", "en", 1998), tf);
    }

    /** Reads with a plain connection, so the finished file is not modified. */
    private static long count(Path db, String sql) throws SQLException {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void firstVersionIsOneAndSelfContained() {
        IndexVersionWriter writer = new IndexVersionWriter(dir);
        assertEquals(0, writer.latestVersion());

        Path db = writer.buildNextVersion(List.of(book(1, Map.of("pride", 3))));

        assertEquals("index-v1.db", db.getFileName().toString());
        assertEquals(1, writer.latestVersion());
        assertFalse(Files.exists(Path.of(db + "-wal")));
        assertFalse(Files.exists(Path.of(db + "-shm")));
        assertFalse(Files.exists(Path.of(db + ".tmp")));
    }

    @Test
    void secondVersionKeepsOldBooksAndLeavesFirstVersionUntouched() throws SQLException {
        IndexVersionWriter writer = new IndexVersionWriter(dir);
        Path v1 = writer.buildNextVersion(List.of(book(1, Map.of("pride", 3))));
        Path v2 = writer.buildNextVersion(List.of(book(2, Map.of("monster", 1))));

        assertEquals("index-v2.db", v2.getFileName().toString());
        assertEquals(2, count(v2, "SELECT COUNT(*) FROM books"));
        assertEquals(1, count(v1, "SELECT COUNT(*) FROM books"));
    }

    @Test
    void reindexingTheSameBookDoesNotDuplicate() throws SQLException {
        IndexVersionWriter writer = new IndexVersionWriter(dir);
        Map<String, Integer> tf = Map.of("pride", 3, "monster", 1);
        writer.buildNextVersion(List.of(book(1, tf)));
        Path v2 = writer.buildNextVersion(List.of(book(1, tf)));

        assertEquals(1, count(v2, "SELECT COUNT(*) FROM books"));
        assertEquals(2, count(v2, "SELECT COUNT(*) FROM postings"));
    }

    @Test
    void manifestDescribesTheFinishedFile() throws Exception {
        IndexVersionWriter writer = new IndexVersionWriter(dir);
        IndexManifest manifest = writer.buildNextVersionWithManifest(
                List.of(book(1, Map.of("pride", 3))));

        Path db = dir.resolve("index-v1.db");
        Path manifestFile = dir.resolve("index-v1.manifest.json");

        assertEquals(1, manifest.version());
        assertEquals("index-v1.db", manifest.file());
        assertEquals(IndexManifestWriter.sha256(db), manifest.sha256());
        assertEquals(Files.size(db), manifest.sizeBytes());
        assertTrue(Files.exists(manifestFile));
        assertEquals(manifest, IndexManifestWriter.read(manifestFile));
    }

    @Test
    void manifestIsNeverOverwritten() {
        IndexVersionWriter writer = new IndexVersionWriter(dir);
        writer.buildNextVersionWithManifest(List.of(book(1, Map.of("pride", 3))));
        Path db = dir.resolve("index-v1.db");

        assertThrows(IllegalStateException.class, () -> IndexManifestWriter.write(db, 1));
    }
}