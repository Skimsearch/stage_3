package com.skimsearch.crawl;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class CrawlingRoleTest {

    @TempDir
    Path tempDir;

    @Test
    void doesNotDownloadReadyBookTwice() throws Exception {

        AtomicInteger downloads =
                new AtomicInteger();

        BookDownloader fakeDownloader = bookId -> {
            downloads.incrementAndGet();

            return new GutenbergClient.BookContent(
                    "Title: Fake",
                    "fake body"
            );
        };

        CrawlingRole role =
                new CrawlingRole(
                        fakeDownloader,
                        new DatalakeWriter(tempDir)
                );

        assertEquals(
                CrawlingRole.Result.DOWNLOADED,
                role.ingestBook(1342)
        );

        assertEquals(
                CrawlingRole.Result.ALREADY_READY,
                role.ingestBook(1342)
        );

        assertEquals(
                1,
                downloads.get()
        );
    }
}