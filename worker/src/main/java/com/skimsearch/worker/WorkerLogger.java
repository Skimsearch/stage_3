package com.skimsearch.worker;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

public final class WorkerLogger implements AutoCloseable {

    private final String workerId;
    private final PrintWriter fileWriter;

    public WorkerLogger(String runId, String workerId) {
        this.workerId = workerId;
        try {
            Path logDir = Path.of("logs", runId);
            Files.createDirectories(logDir);
            Path logFile = logDir.resolve("worker-" + workerId + ".log");
            this.fileWriter = new PrintWriter(
                    Files.newBufferedWriter(logFile, StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                    true
            );
        } catch (IOException e) {
            throw new IllegalStateException("Cannot initialize log file for worker " + workerId, e);
        }
    }

    public synchronized void log(String event, Object... keyValues) {
        StringBuilder sb = new StringBuilder();
        sb.append("ts=").append(Instant.now())
          .append(" event=").append(event)
          .append(" workerId=").append(workerId);

        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            sb.append(' ').append(keyValues[i]).append('=').append(keyValues[i + 1]);
        }

        String line = sb.toString();
        System.out.println(line);
        fileWriter.println(line);
    }

    @Override
    public synchronized void close() {
        fileWriter.flush();
        fileWriter.close();
    }
}