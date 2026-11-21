#!/usr/bin/env python3

import argparse
import hashlib
import json
import time
from pathlib import Path


def sha256(path):
    digest = hashlib.sha256()

    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            digest.update(chunk)

    return digest.hexdigest()


def discover_books(datalake):
    batch = datalake / "batch"
    books = {}

    if not batch.exists():
        return books

    for path in batch.rglob("*"):
        if not path.is_file():
            continue

        name = path.name

        if name.endswith(".header.txt"):
            book_id = name[:-len(".header.txt")]
            kind = "header"
        elif name.endswith(".body.txt"):
            book_id = name[:-len(".body.txt")]
            kind = "body"
        else:
            continue

        try:
            book_id = int(book_id)
        except ValueError:
            continue

        books.setdefault(book_id, {})[kind] = path

    return books


def build_report(datalake):
    books = discover_books(datalake)

    records = []
    ready = 0
    incomplete = 0

    for book_id in sorted(books):
        files = books[book_id]

        header = files.get("header")
        body = files.get("body")

        complete = header is not None and body is not None

        if complete:
            ready += 1
        else:
            incomplete += 1

        records.append({
            "book_id": book_id,
            "status": "READY" if complete else "INCOMPLETE",

            "header": {
                "size_bytes": header.stat().st_size,
                "sha256": sha256(header)
            } if header else None,

            "body": {
                "size_bytes": body.stat().st_size,
                "sha256": sha256(body)
            } if body else None
        })

    return {
        "generated_at_ms": int(time.time() * 1000),
        "datalake": str(datalake),
        "books_found": len(records),
        "ready": ready,
        "incomplete": incomplete,
        "integrity_ok": incomplete == 0,
        "books": records
    }


def main():
    parser = argparse.ArgumentParser(
        description="Check SkimSearch datalake integrity"
    )

    parser.add_argument(
        "--datalake",
        default="data/datalake"
    )

    parser.add_argument(
        "--output"
    )

    args = parser.parse_args()

    report = build_report(Path(args.datalake))

    text = json.dumps(report, indent=2)

    print(text)

    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")

    if not report["integrity_ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()