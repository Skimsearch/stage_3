package com.skimsearch.crawl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class DatalakeWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void writesCompleteBookAndBecomesReady() throws Exception {

        DatalakeWriter writer =
                new DatalakeWriter(tempDir);

        assertEquals(
                DatalakeWriter.State.NOT_FOUND,
                writer.state(1342)
        );

        boolean written = writer.write(
                1342,
                new GutenbergClient.BookContent(
                        "Title: Test",
                        "hello world"
                )
        );

        assertTrue(written);

        assertEquals(
                DatalakeWriter.State.READY,
                writer.state(1342)
        );

        Path folder =
                tempDir.resolve("batch/1000-1999");

        assertTrue(
                Files.exists(
                        folder.resolve("1342.header.txt"))
        );

        assertTrue(
                Files.exists(
                        folder.resolve("1342.body.txt"))
        );
    }

    @Test
    void writingReadyBookIsIdempotent() {

        DatalakeWriter writer =
                new DatalakeWriter(tempDir);

        var content =
                new GutenbergClient.BookContent(
                        "header",
                        "body"
                );

        assertTrue(writer.write(11, content));
        assertFalse(writer.write(11, content));

        assertEquals(
                DatalakeWriter.State.READY,
                writer.state(11)
        );
    }
}