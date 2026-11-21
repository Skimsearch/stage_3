import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

public class AnalyzeWorkerLogs {

    record LogEvent(Instant ts, String event, String workerId, Map<String, String> fields) {}

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Uso: java scripts/worker/AnalyzeWorkerLogs.java <RUN_ID>");
            System.exit(1);
        }

        String runId = args[0];
        Path logDir = Path.of("logs", runId);
        if (!Files.isDirectory(logDir)) {
            System.err.println("No existe el directorio: " + logDir.toAbsolutePath());
            System.exit(1);
        }

        List<LogEvent> events = new ArrayList<>();
        try (Stream<Path> files = Files.list(logDir)) {
            List<Path> logFiles = files
                    .filter(p -> p.getFileName().toString().startsWith("worker-") && p.toString().endsWith(".log"))
                    .toList();
            for (Path file : logFiles) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    LogEvent ev = parseLine(line);
                    if (ev != null) {
                        events.add(ev);
                    }
                }
            }
        }
        events.sort(Comparator.comparing(LogEvent::ts));

        Set<String> workers = new TreeSet<>();
        Set<String> tasks = new TreeSet<>();
        Map<String, Map<String, String>> matrix = new TreeMap<>();
        Map<String, LogEvent> firstClaimByTask = new HashMap<>();
        Map<String, LogEvent> requeueByTask = new HashMap<>();
        List<String> failoverRows = new ArrayList<>();

        for (LogEvent ev : events) {
            if (ev.workerId() != null) {
                workers.add(ev.workerId());
            }
            String taskId = ev.fields().get("taskId");
            if (taskId == null) {
                continue;
            }
            tasks.add(taskId);
            matrix.computeIfAbsent(taskId, k -> new LinkedHashMap<>());

            switch (ev.event()) {
                case "task-claimed" -> {
                    matrix.get(taskId).put(ev.workerId(), "CLAIMED");
                    LogEvent prevClaim = firstClaimByTask.get(taskId);
                    LogEvent req = requeueByTask.get(taskId);
                    if (prevClaim != null && req != null && !prevClaim.workerId().equals(ev.workerId())) {
                        long totalTakeoverMs = Duration.between(prevClaim.ts(), ev.ts()).toMillis();
                        long queueToClaimMs = Duration.between(req.ts(), ev.ts()).toMillis();
                        failoverRows.add(String.format(
                                "| %s | %s -> %s | %s | %s | %d ms | %d ms |",
                                taskId, prevClaim.workerId(), ev.workerId(),
                                req.ts(), ev.ts(), totalTakeoverMs, queueToClaimMs
                        ));
                    } else if (prevClaim == null) {
                        firstClaimByTask.put(taskId, ev);
                    }
                }
                case "task-completed" -> matrix.get(taskId).put(ev.workerId(), "DONE (" + ev.fields().getOrDefault("durationMs", "?") + "ms)");
                case "task-failed" -> matrix.get(taskId).put(ev.workerId(), "FAILED");
                case "task-requeued" -> {
                    requeueByTask.put(taskId, ev);
                    LogEvent prev = firstClaimByTask.get(taskId);
                    if (prev != null && "CLAIMED".equals(matrix.get(taskId).get(prev.workerId()))) {
                        matrix.get(taskId).put(prev.workerId(), "LOST (lease-expired)");
                    }
                }
            }
        }

        System.out.println("\n=== MATRIZ TRABAJADOR x TAREA (" + runId + ") ===");
        System.out.print("| Tarea |");
        for (String w : workers) {
            System.out.print(" " + w + " |");
        }
        System.out.println();

        System.out.print("|---|");
        for (int i = 0; i < workers.size(); i++) {
            System.out.print("---|");
        }
        System.out.println();

        for (String t : tasks) {
            System.out.print("| " + t + " |");
            for (String w : workers) {
                System.out.print(" " + matrix.get(t).getOrDefault(w, "-") + " |");
            }
            System.out.println();
        }

        System.out.println("\n=== TIEMPOS DE RELEVO ANTE CAIDA (FAILOVER) ===");
        if (failoverRows.isEmpty()) {
            System.out.println("No se detectaron tareas retomadas por otro trabajador en este RUN_ID.");
        } else {
            System.out.println("| Tarea | Relevo | ts_requeued | ts_reclaimed | Desde claim original | Desde requeue |");
            System.out.println("|---|---|---|---|---|---|");
            failoverRows.forEach(System.out::println);
        }
    }

    private static LogEvent parseLine(String line) {
        if (!line.startsWith("ts=")) {
            return null;
        }
        String[] parts = line.split(" ");
        Instant ts = null;
        String event = null;
        String workerId = null;
        Map<String, String> fields = new LinkedHashMap<>();

        for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq <= 0) continue;
            String k = part.substring(0, eq);
            String v = part.substring(eq + 1);
            switch (k) {
                case "ts" -> ts = Instant.parse(v);
                case "event" -> event = v;
                case "workerId" -> workerId = v;
                default -> fields.put(k, v);
            }
        }
        return (ts != null && event != null) ? new LogEvent(ts, event, workerId, fields) : null;
    }
}