package com.skimsearch.indexing;

import com.skimsearch.common.IndexManifest;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The indexing role of a worker (design A): builds index versions from datalake
 * books, announces each one as candidate, and periodically tries to promote the
 * candidate to published.
 *
 * Building a version is guarded by a lock, so that two Indexing processes (on the same
 * or on different machines) never build the next version at the same time. Pass a
 * {@link HazelcastBuildLock} for a lock shared by the cluster; without one, an
 * in-process lock is used.
 */
public final class IndexingRole implements AutoCloseable {

    private final VersionedIndexingService service;
    private final IndexPublisher publisher;
    private final Lock buildLock;
    private final ScheduledExecutorService promoter;

    /** Lock only inside this process. */
    public IndexingRole(VersionedIndexingService service, IndexPublisher publisher, Duration promoteEvery) {
        this(service, publisher, promoteEvery, new ReentrantLock());
    }

    public IndexingRole(VersionedIndexingService service, IndexPublisher publisher, Duration promoteEvery,
                        Lock buildLock) {
        this.service = service;
        this.publisher = publisher;
        this.buildLock = buildLock;
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
    // The lock covers build + announce: the next caller starts from the version just finished.
    public IndexManifest indexBooks(List<Integer> bookIds) {
        buildLock.lock();
        try {
            IndexManifest manifest = service.indexBooks(bookIds);
            publisher.announceCandidate(manifest);
            log("event=index-candidate-announced version=" + manifest.version());
            return manifest;
        } finally {
            buildLock.unlock();
        }
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