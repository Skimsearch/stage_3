package com.skimsearch.worker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.skimsearch.crawl.CrawlingRole;
import com.skimsearch.crawl.DatalakeWriter;
import com.skimsearch.crawl.GutenbergClient;
import com.skimsearch.indexing.DatalakeReader;
import com.skimsearch.indexing.HazelcastBuildLock;
import com.skimsearch.indexing.IndexBuilders;
import com.skimsearch.indexing.IndexPublisher;
import com.skimsearch.indexing.IndexingRole;
import com.skimsearch.search.SearchConfig;
import com.skimsearch.search.SearchMain;
public final class WorkerMain {

    public static void main(String[] args) throws Exception {
        WorkerConfig config = WorkerConfig.fromEnvironment();
        WorkerLogger logger = new WorkerLogger(config.runId(), config.workerId());

        Files.createDirectories(config.datalakeDir());
        Files.createDirectories(config.sharedIndexDir());
        Files.createDirectories(config.localDir());

        logger.log("worker-starting",
                "roles", String.join(",", config.roles()),
                "cluster", config.hzClusterName(),
                "hzPort", config.hzPort(),
                "datalakeDir", config.datalakeDir(),
                "sharedIndexDir", config.sharedIndexDir());

        String indexDesign = System.getenv("INDEX_DESIGN");   // null or blank = A
        IndexBuilders.checkDesign(indexDesign);

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(WorkerHazelcast.config(config));
        logger.log("hazelcast-joined",
                "cluster", config.hzClusterName(),
                "members", hz.getCluster().getMembers().size());

        DistributedTaskQueue taskQueue = new DistributedTaskQueue(hz, config.leaseTtlMs(), logger);
        BatchingTaskSink taskSink = new BatchingTaskSink(
                taskQueue,
                config.workerId(),
                config.indexBatchSize(),
                config.indexBatchFlushMs(),
                config.maxAttempts()
        );

        CrawlingRole crawlingRole = null;
        IndexingRole indexingRole = null;

        if (config.roles().contains("search")) {
            SearchMain.start(SearchConfig.fromEnvironment(), hz);
            logger.log("role-started", "role", "search");
        }

        if (config.roles().contains("index")) {
            String design = (indexDesign == null || indexDesign.isBlank()) 
                    ? "A" 
                    : indexDesign.trim().toUpperCase();

            // design B only keeps published-version.txt there, in a folder of its own, so the
            // manifests of a design A run are never restored as B versions
            Path indexDir = "B".equals(design)
                    ? config.sharedIndexDir().resolve("design-b")
                    : config.sharedIndexDir();

            Files.createDirectories(indexDir);
            
            indexingRole = new IndexingRole(
                    IndexBuilders.create(design, new DatalakeReader(config.datalakeDir()), indexDir, hz),
                    new IndexPublisher(hz, indexDir),
                    Duration.ofSeconds(1),
                    HazelcastBuildLock.of(hz)
            );

            logger.log("role-started", "role", "index", "design", design);
        }


        if (config.roles().contains("crawl")) {

            crawlingRole = new CrawlingRole(
                    new GutenbergClient(),
                    new DatalakeWriter(config.datalakeDir())
            );

            logger.log(
                    "role-started",
                    "role", "crawl",
                    "mode", "gutenberg"
            );
        }

        // Sembrado idempotente opcional por variable SEED_BOOKS (ej. "1342,11,84,2701")
        for (int bookId : config.seedBooks()) {
            taskSink.submitIngest(bookId);
        }

        WorkerLoop workerLoop = new WorkerLoop(config, taskQueue, indexingRole, crawlingRole, taskSink, logger);
        workerLoop.start();

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        AtomicBoolean stopped = new AtomicBoolean(false);
        IndexingRole finalIndexingRole = indexingRole;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!stopped.compareAndSet(false, true)) {
                return;
            }
            logger.log("worker-stopping");
            workerLoop.close();
            taskSink.close();

            if (finalIndexingRole != null) {
                try {
                    finalIndexingRole.close();
                    logger.log("role-stopped", "role", "index");
                } catch (Exception e) {
                    logger.log("role-stop-error", "role", "index", "error", e.getMessage());
                }
            }
            try {
                hz.shutdown();
                logger.log("hazelcast-shutdown");
            } catch (Exception e) {
                logger.log("hazelcast-shutdown-error", "error", e.getMessage());
            }
            logger.close();
            shutdownLatch.countDown();
        }, "worker-shutdown-hook"));

        logger.log("worker-ready", "members", hz.getCluster().getMembers().size());
        shutdownLatch.await();
    }
}