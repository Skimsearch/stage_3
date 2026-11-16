package com.skimsearch.indexing;

import com.skimsearch.common.IndexManifest;
import com.skimsearch.indexing.IndexingService.BookNotFoundException;
import com.skimsearch.indexing.IndexingService.BookNotReadyException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class VersionedIndexingServiceTest {

    @TempDir
    Path tmp;

    Path batch;
    Path indexDir;
    IndexVersionWriter writer;
    VersionedIndexingService service;

    @BeforeEach
    void setUp() throws IOException {
        Path lake = tmp.resolve("datalake");
        batch = lake.resolve("batch").resolve("1000-1999");
        Files.createDirectories(batch);
        indexDir = tmp.resolve("index");
        writer = new IndexVersionWriter(indexDir);
        service = new VersionedIndexingService(new DatalakeReader(lake), writer);
    }

    private void writeBook(int id, String body) throws IOException {
        Files.writeString(batch.resolve(id + ".header.txt"),
                "Title: Book " + id + "\nAuthor: Jane Austen\nLanguage: English\n"
                        + "Release date: June 1, 1998 [eBook #" + id + "]\n");
        Files.writeString(batch.resolve(id + ".body.txt"), body);
    }

    @Test
    void buildsOneVersionPerCall() throws IOException {
        writeBook(1342, "pride and prejudice");
        writeBook(1343, "sense and sensibility");

        IndexManifest first = service.indexBooks(List.of(1342));
        IndexManifest second = service.indexBooks(List.of(1343));

        assertEquals(1, first.version());
        assertEquals(2, second.version());
        assertTrue(Files.exists(indexDir.resolve("index-v2.db")));
        assertTrue(Files.exists(indexDir.resolve("index-v2.manifest.json")));
    }

    @Test
    void missingBookWritesNothing() throws IOException {
        writeBook(1342, "pride and prejudice");

        assertThrows(BookNotFoundException.class,
                () -> service.indexBooks(List.of(1342, 1999)));
        assertEquals(0, writer.latestVersion());
    }

    @Test
    void incompleteBookWritesNothing() throws IOException {
        Files.writeString(batch.resolve("1500.header.txt"), "Title: Half\n");

        assertThrows(BookNotReadyException.class,
                () -> service.indexBooks(List.of(1500)));
        assertEquals(0, writer.latestVersion());
    }

        @Test
    void emptyListIsRejectedAndWritesNothing() {
        assertThrows(IllegalArgumentException.class, () -> service.indexBooks(List.of()));
        assertEquals(0, writer.latestVersion());
    }
}