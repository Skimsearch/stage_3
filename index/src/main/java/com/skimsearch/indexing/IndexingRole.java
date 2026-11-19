package com.skimsearch.indexing;

import com.skimsearch.common.IndexManifest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The indexing role of a worker (design A): builds index versions from datalake
 * books, announces each one as candidate, and periodically tries to promote the
 * candidate to published.
 */
public final class IndexingRole implements AutoCloseable {

    private final VersionedIndexingService service;
    private final IndexPublisher publisher;
    private final ScheduledExecutorService promoter;

    public IndexingRole(VersionedIndexingService service, IndexPublisher publisher, Duration promoteEvery) {
        this.service = service;
        this.publisher = publisher;
        this.promoter = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "index-promoter");
            t.setDaemon(true);
            return t;
        });
        publisher.restore();
        long ms = promoteEvery.toMillis();
        promoter.scheduleWithFixedDelay(this::promoteQuietly, ms, ms, TimeUnit.MILLISECONDS);
    }

    /** Builds the next version from these books and announces it as candidate. */
    // synchronized: two concurrent calls in one process would write the same .tmp file.
    // Across machines this is not enough; a Hazelcast FencedLock is needed (later PR).
    public synchronized IndexManifest indexBooks(List<Integer> bookIds) {
        IndexManifest manifest = service.indexBooks(bookIds);
        publisher.announceCandidate(manifest);
        log("event=index-candidate-announced version=" + manifest.version());
        return manifest;
    }

    private void promoteQuietly() {
        try {
            if (publisher.candidateVersion() == 0) {
                // Hazelcast may have restarted while this process kept running: rebuild the keys
                publisher.restore();
            }
            if (publisher.tryPromote()) {
                log("event=index-version-published version=" + publisher.publishedVersion());
            }
        } catch (RuntimeException e) {
            log("event=index-promote-failed error=" + e);
        }
    }

    private static void log(String event) {
        System.out.println("ts=" + Instant.now() + " " + event);
    }

    @Override
    public void close() {
        promoter.shutdownNow();
    }
}