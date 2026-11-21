#!/usr/bin/env python3

import argparse
import concurrent.futures
import csv
import json
import math
import os
import statistics
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path


CSV_COLUMNS = [
    "ts_ms",
    "request_id",
    "query",
    "status",
    "latency_ms",
    "correct",
    "backend",
    "index_version",
    "error",
]


def percentile(values, p):
    if not values:
        return None

    ordered = sorted(values)
    index = max(0, math.ceil(len(ordered) * p) - 1)
    return ordered[index]


def load_queries(path):
    queries = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            queries.append(line)

    if not queries:
        raise ValueError("Query file contains no usable queries")

    return queries


def load_expected(path):
    if path is None:
        return None

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def expected_book_ids(expected, query):
    if expected is None:
        return None

    value = expected.get(query)

    if value is None:
        return None

    if isinstance(value, list):
        return {int(x) for x in value}

    if isinstance(value, dict):
        books = value.get("bookIds", value.get("books"))

        if books is not None:
            return {int(x) for x in books}

    return None


def response_book_ids(payload):
    results = payload.get("results", [])

    ids = set()

    for item in results:
        if "bookId" in item:
            ids.add(int(item["bookId"]))
        elif "book_id" in item:
            ids.add(int(item["book_id"]))

    return ids


def make_request(base_url, query, timeout_s, expected):
    request_id = str(uuid.uuid4())

    encoded_query = urllib.parse.urlencode({"q": query})
    url = f"{base_url.rstrip('/')}/search?{encoded_query}"

    request = urllib.request.Request(
        url,
        method="GET",
        headers={
            "X-Request-Id": request_id,
            "Accept": "application/json",
        },
    )

    start_ns = time.perf_counter_ns()
    ts_ms = int(time.time() * 1000)

    status = 0
    backend = ""
    index_version = ""
    error = ""
    correct = False

    try:
        with urllib.request.urlopen(
            request,
            timeout=timeout_s
        ) as response:

            status = response.status

            backend = response.headers.get(
                "X-Backend-Id", ""
            )

            index_version = response.headers.get(
                "X-Index-Version", ""
            )

            body = response.read().decode("utf-8")

            payload = json.loads(body)

            expected_ids = expected_book_ids(
                expected,
                query
            )

            if expected_ids is None:
                correct = 200 <= status < 300
            else:
                actual_ids = response_book_ids(payload)
                correct = actual_ids == expected_ids

    except urllib.error.HTTPError as e:
        status = e.code

        backend = e.headers.get(
            "X-Backend-Id", ""
        ) if e.headers else ""

        index_version = e.headers.get(
            "X-Index-Version", ""
        ) if e.headers else ""

        error = f"http_{e.code}"

    except TimeoutError:
        error = "timeout"

    except urllib.error.URLError as e:
        if isinstance(e.reason, TimeoutError):
            error = "timeout"
        else:
            error = "connection"

    except json.JSONDecodeError:
        error = "invalid_json"

    except Exception as e:
        error = e.__class__.__name__

    latency_ms = (
        time.perf_counter_ns() - start_ns
    ) / 1_000_000.0

    return {
        "ts_ms": ts_ms,
        "request_id": request_id,
        "query": query,
        "status": status,
        "latency_ms": round(latency_ms, 3),
        "correct": correct,
        "backend": backend,
        "index_version": index_version,
        "error": error,
    }


def worker_loop(
    worker_number,
    stop_event,
    queries,
    base_url,
    timeout_s,
    expected,
    results,
    results_lock,
):
    query_index = worker_number

    while not stop_event.is_set():

        query = queries[
            query_index % len(queries)
        ]

        query_index += 1

        result = make_request(
            base_url,
            query,
            timeout_s,
            expected,
        )

        with results_lock:
            results.append(result)


def write_csv(path, results):
    with open(
        path,
        "w",
        encoding="utf-8",
        newline=""
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=CSV_COLUMNS
        )

        writer.writeheader()
        writer.writerows(results)


def build_summary(
    run_id,
    duration_s,
    concurrency,
    warmup_s,
    timeout_s,
    results
):
    latencies = [
        float(r["latency_ms"])
        for r in results
        if not r["error"]
    ]

    total = len(results)

    correct = sum(
        1 for r in results
        if r["correct"] is True
    )

    errors = sum(
        1 for r in results
        if r["error"]
    )

    timeouts = sum(
        1 for r in results
        if r["error"] == "timeout"
    )

    successes = sum(
        1 for r in results
        if 200 <= int(r["status"]) < 300
    )

    return {
        "run_id": run_id,
        "duration_s": duration_s,
        "warmup_s": warmup_s,
        "concurrency": concurrency,
        "timeout_s": timeout_s,

        "requests": total,
        "successes": successes,
        "correct": correct,
        "errors": errors,
        "timeouts": timeouts,

        "correct_queries_per_second":
            round(correct / duration_s, 3)
            if duration_s > 0 else 0,

        "requests_per_second":
            round(total / duration_s, 3)
            if duration_s > 0 else 0,

        "mean_latency_ms":
            round(statistics.mean(latencies), 3)
            if latencies else None,

        "p95_latency_ms":
            round(percentile(latencies, 0.95), 3)
            if latencies else None,

        "max_latency_ms":
            round(max(latencies), 3)
            if latencies else None,
    }


def run_phase(
    duration_s,
    concurrency,
    queries,
    base_url,
    timeout_s,
    expected,
    collect
):
    results = []
    results_lock = threading.Lock()
    stop_event = threading.Event()

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=concurrency
    ) as executor:

        futures = []

        for worker_number in range(concurrency):
            futures.append(
                executor.submit(
                    worker_loop,
                    worker_number,
                    stop_event,
                    queries,
                    base_url,
                    timeout_s,
                    expected,
                    results,
                    results_lock,
                )
            )

        time.sleep(duration_s)

        stop_event.set()

        for future in futures:
            future.result()

    if collect:
        return results

    return []


def main():

    parser = argparse.ArgumentParser(
        description="SkimSearch Stage 3 HTTP load generator"
    )

    parser.add_argument(
        "--url",
        default="http://localhost",
        help="Nginx/gateway base URL"
    )

    parser.add_argument(
        "--queries",
        required=True,
        help="Query workload file"
    )

    parser.add_argument(
        "--expected",
        default=None,
        help="Expected results JSON"
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=10
    )

    parser.add_argument(
        "--warmup",
        type=int,
        default=10,
        help="Warm-up duration in seconds"
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=30,
        help="Measurement duration in seconds"
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=2.0
    )

    parser.add_argument(
        "--run-id",
        default=None
    )

    args = parser.parse_args()

    if args.concurrency < 1:
        raise ValueError(
            "concurrency must be >= 1"
        )

    if args.duration < 1:
        raise ValueError(
            "duration must be >= 1"
        )

    run_id = args.run_id or (
        time.strftime("%Y%m%d-%H%M%S")
        + "-"
        + uuid.uuid4().hex[:6]
    )

    queries = load_queries(args.queries)
    expected = load_expected(args.expected)

    output_dir = (
        Path("results") / run_id
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=False
    )

    print(f"Run ID: {run_id}")
    print(f"Gateway: {args.url}")
    print(
        f"Concurrency: {args.concurrency}"
    )
    print(f"Warm-up: {args.warmup}s")
    print(
        f"Measurement: {args.duration}s"
    )
    print(f"Timeout: {args.timeout}s")

    if args.warmup > 0:
        print("Warm-up...")

        run_phase(
            args.warmup,
            args.concurrency,
            queries,
            args.url,
            args.timeout,
            expected,
            False,
        )

    print("Measurement...")

    results = run_phase(
        args.duration,
        args.concurrency,
        queries,
        args.url,
        args.timeout,
        expected,
        True,
    )

    requests_csv = (
        output_dir / "requests.csv"
    )

    summary_json = (
        output_dir / "summary.json"
    )

    write_csv(
        requests_csv,
        results
    )

    summary = build_summary(
        run_id,
        args.duration,
        args.concurrency,
        args.warmup,
        args.timeout,
        results
    )

    with open(
        summary_json,
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            summary,
            f,
            indent=2
        )

    print()
    print(json.dumps(summary, indent=2))
    print()
    print(
        f"Raw results: {requests_csv}"
    )
    print(
        f"Summary: {summary_json}"
    )


if __name__ == "__main__":
    main()