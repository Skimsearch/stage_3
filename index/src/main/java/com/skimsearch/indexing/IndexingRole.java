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
 * The indexing role of a worker: builds index versions from datalake
 * books (with the builder of design A or B), announces each one as candidate, and periodically tries to promote the
 * candidate to published.
 *
 * Building a version is guarded by a lock, so that two Indexing processes (on the same
 * or on different machines) never build the next version at the same time. Pass a
 * {@link HazelcastBuildLock} for a lock shared by the cluster; without one, an
 * in-process lock is used.
 */
public final class IndexingRole implements AutoCloseable {

    private final IndexBuilder service;
    private final IndexPublisher publisher;
    private final Lock buildLock;
    private final ScheduledExecutorService promoter;

    /** Lock only inside this process. */
    public IndexingRole(IndexBuilder service, IndexPublisher publisher, Duration promoteEvery) {
        this(service, publisher, promoteEvery, new ReentrantLock());
    }

    public IndexingRole(IndexBuilder service, IndexPublisher publisher, Duration promoteEvery,
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
        long waitStart = System.nanoTime();
        buildLock.lock();
        try {
            long buildStart = System.nanoTime();
            IndexManifest manifest = service.indexBooks(bookIds);
            long buildNanos = System.nanoTime() - buildStart;
            publisher.announceCandidate(manifest);
            log("event=index-candidate-announced version=" + manifest.version());
            int built = service.booksInLastBuild(bookIds.size());
            log(buildFinishedEvent(manifest.version(), bookIds.size(), built, buildNanos, buildStart - waitStart));
            return manifest;
        } finally {
            buildLock.unlock();
        }
    }

    /**
     * Indexing rate of one build: books actually built divided by the build time (reading the books,
     * tokenising and writing the version; the lock wait and the announcement are not included).
     * requested = books of the task; books = books built (more than requested when design B rebuilds
     * everything after a Hazelcast restart, then rebuild=true). The lock wait is logged apart, so
     * contention between indexers is visible too.
     */
    static String buildFinishedEvent(long version, int requested, int books, long buildNanos, long lockWaitNanos) {
        double seconds = buildNanos / 1e9;
        double rate = seconds > 0 ? books / seconds : 0;
        return String.format(java.util.Locale.ROOT,
                "event=index-build-finished version=%d requested=%d books=%d rebuild=%b buildMs=%.1f "
                        + "lockWaitMs=%.1f booksPerSecond=%.2f",
                version, requested, books, books > requested, buildNanos / 1e6, lockWaitNanos / 1e6, rate);
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