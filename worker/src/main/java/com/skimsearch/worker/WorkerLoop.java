package com.skimsearch.worker;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.skimsearch.common.TaskSink;
import com.skimsearch.indexing.IndexingRole;
import com.skimsearch.crawl.CrawlingRole;

public final class WorkerLoop implements AutoCloseable {

    private final WorkerConfig config;
    private final DistributedTaskQueue queue;
    private final IndexingRole indexingRole;
    private final CrawlingRole crawlingRole;
    private final TaskSink taskSink;
    private final WorkerLogger logger;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicReference<Task> currentTask = new AtomicReference<>(null);
    private final ScheduledExecutorService maintenanceExecutor;
    private final Thread workerThread;

    public WorkerLoop(WorkerConfig config,
                      DistributedTaskQueue queue,
                      IndexingRole indexingRole,
                      CrawlingRole crawlingRole,
                      TaskSink taskSink,
                      WorkerLogger logger) {
        this.config = config;
        this.queue = queue;
        this.indexingRole = indexingRole;
        this.crawlingRole = crawlingRole;
        this.taskSink = taskSink;
        this.logger = logger;

        this.maintenanceExecutor = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "worker-maintenance-" + config.workerId());
            t.setDaemon(true);
            return t;
        });

        // 1. Renovación periódica del lease de la tarea en curso
        this.maintenanceExecutor.scheduleAtFixedRate(() -> {
            Task active = currentTask.get();
            if (active != null && running.get()) {
                queue.renewLease(active.getId(), config.workerId());
            }
        }, config.leaseRenewMs(), config.leaseRenewMs(), TimeUnit.MILLISECONDS);

        // 2. Reaper distribuido en todos los nodos
        this.maintenanceExecutor.scheduleAtFixedRate(() -> {
            if (running.get()) {
                try {
                    queue.reapExpiredLeases(config.workerId());
                } catch (Exception e) {
                    logger.log("reaper-error", "error", e.getMessage());
                }
            }
        }, config.reaperIntervalMs(), config.reaperIntervalMs(), TimeUnit.MILLISECONDS);

        this.workerThread = new Thread(this::runLoop, "worker-loop-" + config.workerId());
    }

    public void start() {
        Set<String> roles = config.roles();
        if (roles.contains("crawl") || roles.contains("index")) {
            workerThread.start();
            logger.log("worker-loop-started",
                    "leaseTtlMs", config.leaseTtlMs(),
                    "leaseRenewMs", config.leaseRenewMs(),
                    "reaperIntervalMs", config.reaperIntervalMs());
        } else {
            logger.log("worker-loop-idle", "reason", "no-queue-consumer-roles");
        }
    }

    private void runLoop() {
        while (running.get()) {
            try {
                Optional<Task> claimed = queue.claimNext(config.workerId(), config.roles(), 500);
                if (claimed.isEmpty()) {
                    continue;
                }

                Task task = claimed.get();
                currentTask.set(task);
                long startNs = System.nanoTime();

                try {
                    executeTask(task);
                    long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
                    queue.complete(task, config.workerId(), durationMs);
                } catch (Exception e) {
                    logger.log("task-execution-error",
                            "taskId", task.getId(),
                            "type", task.getType(),
                            "error", e.getMessage());
                    queue.failOrRequeue(task, config.workerId(), "execution-error:" + e.getClass().getSimpleName());
                } finally {
                    currentTask.set(null);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                if (running.get()) {
                    logger.log("worker-loop-error", "error", e.getMessage());
                }
            }
        }
    }

    private void executeTask(Task task) throws Exception {
        if ("index".equals(task.getType())) {
            if (indexingRole == null) {
                throw new IllegalStateException("IndexingRole is not active on this worker");
            }
            indexingRole.indexBooks(task.getBookIds());
            return;
        }

    if ("ingest".equals(task.getType())) {
        if (crawlingRole == null) {
            throw new IllegalStateException(
                    "CrawlingRole is not active on this worker"
            );
        }

        for (int bookId : task.getBookIds()) {
            CrawlingRole.Result result =
                    crawlingRole.ingestBook(bookId);

            logger.log(
                    "crawl-completed",
                    "bookId", bookId,
                    "result", result.name()
            );

            /*
            * Always announce a READY book, including ALREADY_READY.
            *
            * A worker may fail after publishing the document but before
            * completing the handoff to Indexing. On retry, the next worker
            * observes ALREADY_READY and must still resume the pipeline.
            */
            taskSink.bookReady(bookId);
        }

        return;
    }

        throw new IllegalArgumentException("Unknown task type: " + task.getType());
    }

    @Override
    public void close() {
        running.set(false);
        maintenanceExecutor.shutdownNow();
        workerThread.interrupt();
        try {
            workerThread.join(2000);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}