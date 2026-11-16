package com.skimsearch.search;

import com.google.gson.Gson;
import com.skimsearch.common.Filters;
import com.skimsearch.common.IndexManifest;
import com.skimsearch.common.IndexUnavailableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VersionedIndexTest {

    @TempDir
    Path shared;
    @TempDir
    Path local;

    final AtomicLong published = new AtomicLong(1);
    VersionedIndex index;

    @AfterEach
    void close() {
        if (index != null) {
            index.close();
        }
    }

    /** Writes index-v{version}.db with `books` books for the term "love", plus its manifest. */
    static void publishFiles(Path dir, long version, int books) throws Exception {
        Path db = dir.resolve(IndexManifest.dbFileName(version));
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db);
             Statement st = c.createStatement()) {
            st.executeUpdate("""
                    CREATE TABLE books (book_id INTEGER PRIMARY KEY, title TEXT, author TEXT,
                      language TEXT, year INTEGER, term_count INTEGER, indexed_at TEXT)""");
            st.executeUpdate("CREATE TABLE postings (term TEXT, book_id INTEGER, tf INTEGER)");
            for (int id = 1; id <= books; id++) {
                st.executeUpdate("INSERT INTO books VALUES (" + id + ", 'T', 'A', 'en', 2000, 1, 't')");
                st.executeUpdate("INSERT INTO postings VALUES ('love', " + id + ", 1)");
            }
        }
        writeManifest(dir, new IndexManifest(version, db.getFileName().toString(),
                VersionedIndex.sha256(db), Files.size(db), System.currentTimeMillis()));
    }

    static void writeManifest(Path dir, IndexManifest manifest) throws Exception {
        Files.writeString(dir.resolve(IndexManifest.manifestFileName(manifest.version())),
                new Gson().toJson(manifest));
    }

    private VersionedIndex open() {
        index = new VersionedIndex(shared, local, published::get, Duration.ofHours(1));
        return index;
    }

    @Test
    void servesThePublishedVersionFromALocalCopy() throws Exception {
        publishFiles(shared, 1, 3);
        open();
        assertEquals(1, index.servedVersion());
        assertEquals(3, index.count("love", Filters.none()));
        assertTrue(Files.isRegularFile(local.resolve("index-v1.db")), "must read a local copy");
    }

    @Test
    void loadsTheCandidateButKeepsServingThePublishedOne() throws Exception {
        publishFiles(shared, 1, 1);
        open();
        publishFiles(shared, 2, 2);   // candidate written by Indexing
        index.refresh();

        assertEquals(2, index.highestLoadedVersion(), "candidate loaded in advance");
        assertEquals(1, index.servedVersion(), "but not served before it is published");
        assertEquals(1, index.count("love", Filters.none()));

        published.set(2);             // Indexing promotes it
        assertEquals(2, index.servedVersion(), "switch is immediate, no copy needed");
        assertEquals(2, index.count("love", Filters.none()));
    }

    @Test
    void ignoresVersionsWithoutManifest() throws Exception {
        publishFiles(shared, 1, 1);
        Files.copy(shared.resolve("index-v1.db"), shared.resolve("index-v2.db")); // no manifest yet
        open();
        assertEquals(1, index.highestLoadedVersion());
    }

    @Test
    void rejectsACopyWhoseChecksumDoesNotMatch() throws Exception {
        publishFiles(shared, 1, 1);
        Files.writeString(shared.resolve("index-v1.db"), "corrupted", java.nio.file.StandardOpenOption.APPEND);
        open();
        assertEquals(0, index.servedVersion());
        assertFalse(Files.exists(local.resolve("index-v1.db")));
        assertThrows(IndexUnavailableException.class, () -> index.count("love", Filters.none()));
    }

    @Test
    void rejectsAManifestThatPointsToAnotherVersionsFile() throws Exception {
        publishFiles(shared, 1, 1);
        IndexManifest v1 = new Gson().fromJson(
                Files.readString(shared.resolve("index-v1.manifest.json")), IndexManifest.class);
        writeManifest(shared, new IndexManifest(2, v1.file(), v1.sha256(), v1.sizeBytes(), 0));
        open();
        assertEquals(1, index.highestLoadedVersion(), "v2 manifest naming index-v1.db is ignored");
    }

    @Test
    void notReadyWhilePublishedVersionIsUnknown() throws Exception {
        publishFiles(shared, 1, 1);
        index = new VersionedIndex(shared, local,
                () -> { throw new IndexUnavailableException("hazelcast down"); }, Duration.ofHours(1));
        assertEquals(1, index.highestLoadedVersion(), "loads anyway, to be ready sooner");
        assertEquals(0, index.servedVersion(), "but serves nothing until the published version is known");
    }

    @Test
    void deletesOldLocalCopiesButKeepsThePreviousOne() throws Exception {
        publishFiles(shared, 1, 1);
        open();
        for (long v = 2; v <= 3; v++) {
            publishFiles(shared, v, (int) v);
            published.set(v);
            index.refresh();
        }
        assertEquals(3, index.servedVersion());
        assertFalse(Files.exists(local.resolve("index-v1.db")), "v1 deleted");
        assertTrue(Files.exists(local.resolve("index-v2.db")), "v2 kept for requests still running");
    }

    @Test
    void reusesAVerifiedLocalCopyAfterRestart() throws Exception {
        publishFiles(shared, 1, 1);
        open().close();
        long modified = Files.getLastModifiedTime(local.resolve("index-v1.db")).toMillis();
        Thread.sleep(20);
        open();
        assertEquals(1, index.servedVersion());
        assertEquals(modified, Files.getLastModifiedTime(local.resolve("index-v1.db")).toMillis(),
                "not copied again");
    }
}