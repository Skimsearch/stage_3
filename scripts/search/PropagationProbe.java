import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures how fast changes in the Search pool reach the gateway: an instance joining,
 * leaving and coming back. JDK only, no build needed:
 *
 *   java scripts/search/PropagationProbe.java --gateway http://GW:8080 --consul http://GW:8500 \
 *        --run-id A-search-2nodes-run01 [--query alpha] [--rate 20] [--out results]
 *
 * While it runs it
 *   1. sends GET /search to the gateway at a fixed rate (each with X-Request-Id = runId-N),
 *   2. polls Consul every 100 ms and records every health change of each "search" instance,
 *   3. reads lines from the console: each line is a timestamped mark ("kill w2", "start w3");
 *      "end" stops the run.
 * Every timestamp comes from this machine's clock, so there is no skew between machines.
 *
 * Output in <out>/<runId>/: requests.csv, consul.csv, marks.csv and summary.txt.
 */
public class PropagationProbe {

    record Request(long tsMs, String id, int status, double latencyMs, String backend, String version, String error) {
    }

    record HealthChange(long tsMs, String serviceId, String worker, String from, String to) {
    }

    record Mark(long tsMs, String label) {
    }

    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    public static void main(String[] args) throws Exception {
        Map<String, String> opt = options(args);
        String gateway = required(opt, "gateway");
        String consul = required(opt, "consul");
        String runId = required(opt, "run-id");
        String query = opt.getOrDefault("query", "alpha");
        int rate = Integer.parseInt(opt.getOrDefault("rate", "20"));
        Path outDir = Path.of(opt.getOrDefault("out", "results")).resolve(runId);
        Files.createDirectories(outDir);

        var requests = new ConcurrentLinkedQueue<Request>();
        var changes = new ConcurrentLinkedQueue<HealthChange>();
        var marks = new ConcurrentLinkedQueue<Mark>();
        AtomicLong seq = new AtomicLong();
        URI searchUri = URI.create(gateway + "/search?q=" + URLEncoder.encode(query, StandardCharsets.UTF_8));

        ScheduledExecutorService pool = Executors.newScheduledThreadPool(2);
        // the timer only launches requests; a slow one (retry, timeout) must not delay the next
        ExecutorService senders = Executors.newCachedThreadPool();
        pool.scheduleAtFixedRate(() -> senders.submit(() -> requests.add(probe(searchUri, runId + "-" + seq.incrementAndGet()))),
                0, 1_000_000 / rate, TimeUnit.MICROSECONDS);

        Map<String, String[]> lastState = new HashMap<>();   // serviceId -> {worker, status}
        pool.scheduleWithFixedDelay(() -> watchConsul(consul, lastState, changes), 0, 100, TimeUnit.MILLISECONDS);

        System.out.println("probe running: run=" + runId + " rate=" + rate + "/s query=" + query);
        System.out.println("type a mark and Enter (e.g. 'kill w2'); type 'end' to finish");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.equalsIgnoreCase("end")) {
                break;
            }
            if (!line.isEmpty()) {
                marks.add(new Mark(System.currentTimeMillis(), line));
                System.out.println("mark: " + line);
            }
        }
        marks.add(new Mark(System.currentTimeMillis(), "end"));
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        senders.shutdown();
        senders.awaitTermination(5, TimeUnit.SECONDS);   // let requests in flight finish

        List<Request> reqs = sorted(requests, Request::tsMs);
        List<HealthChange> chg = sorted(changes, HealthChange::tsMs);
        List<Mark> mks = sorted(marks, Mark::tsMs);
        writeCsvs(outDir, reqs, chg, mks);
        String summary = summary(runId, rate, query, reqs, chg, mks);
        Files.writeString(outDir.resolve("summary.txt"), summary);
        System.out.println(summary);
        System.out.println("results in " + outDir.toAbsolutePath());
    }

    // ------------------------------------------------------------------ probing

    private static Request probe(URI uri, String id) {
        long start = System.nanoTime();
        long ts = System.currentTimeMillis();
        try {
            HttpResponse<Void> r = HTTP.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(2))
                    .header("X-Request-Id", id).GET().build(), HttpResponse.BodyHandlers.discarding());
            return new Request(ts, id, r.statusCode(), (System.nanoTime() - start) / 1e6,
                    r.headers().firstValue("X-Backend-Id").orElse("-"),
                    r.headers().firstValue("X-Index-Version").orElse("-"), "");
        } catch (HttpTimeoutException e) {   // includes connect timeouts
            return new Request(ts, id, 0, (System.nanoTime() - start) / 1e6, "-", "-", "timeout");
        } catch (IOException | InterruptedException e) {
            return new Request(ts, id, 0, (System.nanoTime() - start) / 1e6, "-", "-", "connection");
        }
    }

    /** Records every change of each instance's aggregated health (passing / warning / critical / absent). */
    @SuppressWarnings("unchecked")
    private static void watchConsul(String consul, Map<String, String[]> last, ConcurrentLinkedQueue<HealthChange> out) {
        long ts = System.currentTimeMillis();
        Map<String, String[]> now = new HashMap<>();
        try {
            HttpResponse<String> r = HTTP.send(HttpRequest.newBuilder(URI.create(consul + "/v1/health/service/search"))
                    .timeout(Duration.ofSeconds(2)).GET().build(), HttpResponse.BodyHandlers.ofString());
            for (Object entry : (List<Object>) Json.parse(r.body())) {
                Map<String, Object> e = (Map<String, Object>) entry;
                Map<String, Object> service = (Map<String, Object>) e.get("Service");
                String id = (String) service.get("ID");
                Object meta = service.get("Meta");
                String worker = meta instanceof Map<?, ?> m && m.get("workerId") != null ? (String) m.get("workerId") : id;
                String status = "passing";
                for (Object c : (List<Object>) e.get("Checks")) {
                    status = worse(status, (String) ((Map<String, Object>) c).get("Status"));
                }
                now.put(id, new String[]{worker, status});
            }
        } catch (Exception e) {
            return;   // Consul unreachable: keep the last known state, try again in 100 ms
        }
        for (var e : now.entrySet()) {
            String[] before = last.get(e.getKey());
            String from = before == null ? "absent" : before[1];
            if (!from.equals(e.getValue()[1])) {
                out.add(new HealthChange(ts, e.getKey(), e.getValue()[0], from, e.getValue()[1]));
            }
        }
        for (var e : last.entrySet()) {
            if (!now.containsKey(e.getKey())) {
                out.add(new HealthChange(ts, e.getKey(), e.getValue()[0], e.getValue()[1], "absent"));
            }
        }
        last.clear();
        last.putAll(now);
    }

    private static String worse(String a, String b) {
        List<String> order = List.of("passing", "warning", "critical");
        return order.indexOf(b) > order.indexOf(a) ? b : a;
    }

    // ------------------------------------------------------------------ output

    private static void writeCsvs(Path dir, List<Request> reqs, List<HealthChange> chg, List<Mark> mks) throws IOException {
        StringBuilder r = new StringBuilder("ts_ms,request_id,status,latency_ms,backend,index_version,error\n");
        for (Request q : reqs) {
            r.append(String.format(Locale.ROOT, "%d,%s,%d,%.1f,%s,%s,%s%n",
                    q.tsMs(), q.id(), q.status(), q.latencyMs(), q.backend(), q.version(), q.error()));
        }
        Files.writeString(dir.resolve("requests.csv"), r);

        StringBuilder c = new StringBuilder("ts_ms,service_id,worker,from,to\n");
        for (HealthChange h : chg) {
            c.append(h.tsMs()).append(',').append(h.serviceId()).append(',').append(h.worker()).append(',')
                    .append(h.from()).append(',').append(h.to()).append('\n');
        }
        Files.writeString(dir.resolve("consul.csv"), c);

        StringBuilder m = new StringBuilder("ts_ms,label\n");
        for (Mark k : mks) {
            m.append(k.tsMs()).append(',').append(k.label().replace(',', ' ')).append('\n');
        }
        Files.writeString(dir.resolve("marks.csv"), m);
    }

    /**
     * For every health change of an instance: the mark before it (what the operator did),
     * how long Consul took to see it, and when the gateway started or stopped sending it traffic.
     */
    private static String summary(String runId, int rate, String query, List<Request> reqs,
                                  List<HealthChange> chg, List<Mark> mks) {
        long t0 = reqs.isEmpty() ? System.currentTimeMillis() : reqs.get(0).tsMs();
        StringBuilder s = new StringBuilder();
        s.append("run ").append(runId).append(" | rate ").append(rate).append("/s | query '").append(query).append("'\n");
        s.append("times in seconds since the first request; all from this machine's clock\n\n");

        long ok = reqs.stream().filter(q -> q.status() == 200).count();
        Map<String, Long> failures = new TreeMap<>();
        for (Request q : reqs) {
            if (q.status() != 200) {
                failures.merge(q.error().isEmpty() ? "http_" + q.status() : q.error(), 1L, Long::sum);
            }
        }
        s.append(String.format(Locale.ROOT, "requests %d | ok %d | failed %d %s%n",
                reqs.size(), ok, reqs.size() - ok, failures.isEmpty() ? "" : failures));
        Map<String, Long> perBackend = new TreeMap<>();
        reqs.stream().filter(q -> q.status() == 200).forEach(q -> perBackend.merge(q.backend(), 1L, Long::sum));
        s.append("served by backend ").append(perBackend).append('\n');
        s.append(String.format(Locale.ROOT, "longest time without a successful response: %.2f s%n%n",
                longestGap(reqs) / 1000.0));

        s.append("timeline\n");
        List<Object[]> events = new ArrayList<>();
        mks.forEach(k -> events.add(new Object[]{k.tsMs(), "mark   " + k.label()}));
        chg.forEach(h -> events.add(new Object[]{h.tsMs(), "consul " + h.worker() + " " + h.from() + " -> " + h.to()}));
        events.sort((a, b) -> Long.compare((long) a[0], (long) b[0]));
        for (Object[] e : events) {
            s.append(String.format(Locale.ROOT, "  %8.2f  %s%n", ((long) e[0] - t0) / 1000.0, e[1]));
        }

        s.append("\npropagation per health change\n");
        for (HealthChange h : chg) {
            if (h.from().equals("absent") && h.tsMs() - t0 < 300) {
                continue;   // state when the probe started, not a change (still shown in the timeline)
            }
            Mark before = null;
            for (Mark k : mks) {
                if (k.tsMs() <= h.tsMs()) {
                    before = k;
                }
            }
            String detect = before == null ? "-" : String.format(Locale.ROOT, "%.2f s after mark '%s'",
                    (h.tsMs() - before.tsMs()) / 1000.0, before.label());
            // until this instance's next health change (e.g. a restart after a crash)
            long until = chg.stream().filter(o -> o.worker().equals(h.worker()) && o.tsMs() > h.tsMs())
                    .map(HealthChange::tsMs).findFirst().orElse(Long.MAX_VALUE);
            String routing;
            if (h.to().equals("passing")) {
                Long first = reqs.stream().filter(q -> q.status() == 200 && q.backend().equals(h.worker())
                        && q.tsMs() >= h.tsMs()).map(Request::tsMs).findFirst().orElse(null);
                routing = first == null ? "never received traffic"
                        : String.format(Locale.ROOT, "first request served %.2f s after Consul", (first - h.tsMs()) / 1000.0);
            } else {
                long after = reqs.stream().filter(q -> q.status() == 200 && q.backend().equals(h.worker())
                        && q.tsMs() > h.tsMs() && q.tsMs() < until).count();
                Long last = reqs.stream().filter(q -> q.status() == 200 && q.backend().equals(h.worker())
                        && q.tsMs() <= h.tsMs()).map(Request::tsMs).reduce((a, b) -> b).orElse(null);
                routing = (last == null ? "no traffic before" : String.format(Locale.ROOT,
                        "last request served %.2f s before Consul", (h.tsMs() - last) / 1000.0))
                        + ", " + after + " requests served while not passing";
            }
            s.append(String.format(Locale.ROOT, "  %s %s -> %s: Consul %s; %s%n",
                    h.worker(), h.from(), h.to(), detect, routing));
        }
        return s.toString();
    }

    private static long longestGap(List<Request> reqs) {
        long gap = 0;
        Long lastOk = null;
        for (Request q : reqs) {
            if (q.status() == 200) {
                if (lastOk != null) {
                    gap = Math.max(gap, q.tsMs() - lastOk);
                }
                lastOk = q.tsMs();
            }
        }
        return gap;
    }

    // ------------------------------------------------------------------ helpers

    private static <T> List<T> sorted(ConcurrentLinkedQueue<T> q, java.util.function.ToLongFunction<T> key) {
        List<T> list = new ArrayList<>(q);
        list.sort((a, b) -> Long.compare(key.applyAsLong(a), key.applyAsLong(b)));
        return list;
    }

    private static Map<String, String> options(String[] args) {
        Map<String, String> opt = new LinkedHashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--")) {
                usage("unexpected argument " + args[i]);
            }
            opt.put(args[i].substring(2), args[i + 1]);
        }
        return opt;
    }

    private static String required(Map<String, String> opt, String name) {
        String v = opt.get(name);
        if (v == null || v.isBlank()) {
            usage("--" + name + " is required");
        }
        return v;
    }

    private static void usage(String problem) {
        System.err.println(problem);
        System.err.println("usage: java PropagationProbe.java --gateway URL --consul URL --run-id ID"
                + " [--query alpha] [--rate 20] [--out results]");
        System.exit(2);
    }

    /** Minimal JSON reader (objects, arrays, strings, numbers, booleans, null), enough for Consul. */
    static final class Json {
        private final String s;
        private int i;

        private Json(String s) {
            this.s = s;
        }

        static Object parse(String text) {
            Json j = new Json(text);
            Object v = j.value();
            j.ws();
            if (j.i != j.s.length()) {
                throw new IllegalArgumentException("trailing data at " + j.i);
            }
            return v;
        }

        private Object value() {
            ws();
            char c = s.charAt(i);
            if (c == '{') {
                Map<String, Object> m = new LinkedHashMap<>();
                i++;
                ws();
                if (s.charAt(i) == '}') {
                    i++;
                    return m;
                }
                while (true) {
                    ws();
                    String k = string();
                    ws();
                    expect(':');
                    m.put(k, value());
                    ws();
                    if (s.charAt(i) == ',') {
                        i++;
                    } else {
                        expect('}');
                        return m;
                    }
                }
            }
            if (c == '[') {
                List<Object> l = new ArrayList<>();
                i++;
                ws();
                if (s.charAt(i) == ']') {
                    i++;
                    return l;
                }
                while (true) {
                    l.add(value());
                    ws();
                    if (s.charAt(i) == ',') {
                        i++;
                    } else {
                        expect(']');
                        return l;
                    }
                }
            }
            if (c == '"') {
                return string();
            }
            if (s.startsWith("true", i)) {
                i += 4;
                return Boolean.TRUE;
            }
            if (s.startsWith("false", i)) {
                i += 5;
                return Boolean.FALSE;
            }
            if (s.startsWith("null", i)) {
                i += 4;
                return null;
            }
            int start = i;
            while (i < s.length() && "+-0123456789.eE".indexOf(s.charAt(i)) >= 0) {
                i++;
            }
            return Double.parseDouble(s.substring(start, i));
        }

        private String string() {
            expect('"');
            StringBuilder b = new StringBuilder();
            while (true) {
                char c = s.charAt(i++);
                if (c == '"') {
                    return b.toString();
                }
                if (c == '\\') {
                    char e = s.charAt(i++);
                    switch (e) {
                        case 'n' -> b.append('\n');
                        case 't' -> b.append('\t');
                        case 'r' -> b.append('\r');
                        case 'b' -> b.append('\b');
                        case 'f' -> b.append('\f');
                        case 'u' -> {
                            b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                            i += 4;
                        }
                        default -> b.append(e);
                    }
                } else {
                    b.append(c);
                }
            }
        }

        private void expect(char c) {
            if (s.charAt(i) != c) {
                throw new IllegalArgumentException("expected '" + c + "' at " + i);
            }
            i++;
        }

        private void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
                i++;
            }
        }
    }
}