# Crawling Role

The Crawling module implements the document acquisition role used by distributed
Stage 3 workers.

## Responsibility

For each ingest task, the crawler:

1. receives a Project Gutenberg book ID;
2. checks whether the book is already complete in the shared datalake;
3. downloads the book when necessary;
4. separates the Gutenberg header and body;
5. publishes both files in the shared datalake;
6. returns control to the Worker so an indexing task can be generated.

## Datalake contract

Books are stored as:

DATALAKE_DIR/
└── batch/
    └── {start}-{end}/
        ├── {book_id}.header.txt
        └── {book_id}.body.txt

where:

start = (book_id / 1000) * 1000
end   = start + 999

This is the same contract consumed by the Indexing module.

## Safe publication

Files are first written using unique temporary names and are then moved to the
final names.

A book is considered READY only when both final files exist.

Unique temporary names prevent two independent attempts from using the same
temporary file.

## Idempotency

If both final files already exist, the crawler does not download the book again.

This is important when a worker dies after publishing the files but before the
distributed task is acknowledged.

When another worker retries the task, it detects that the book is already READY
and continues the workflow without downloading it again.

## Distributed task recovery

Task ownership and retries are handled by the Worker module using Hazelcast
queues and leases.

The Crawling module does not implement its own distributed queue.

A worker with the crawl role executes an ingest task through CrawlingRole.

After successful ingestion, the Worker calls:

taskSink.bookReady(bookId);

This allows the corresponding indexing task to be created.

If a worker fails while processing an ingest task, its lease eventually expires
and another worker can claim the task.

Because crawling is idempotent, retrying an already completed download does not
duplicate the stored book.

## Main classes

- BookDownloader: abstraction used to test CrawlingRole without requiring Internet access.
- GutenbergClient: downloads Project Gutenberg books and separates header and body.
- DatalakeWriter: publishes complete header/body files into the shared datalake.
- CrawlingRole: idempotent crawling operation executed by workers.

## Tests

From the repository root, run:

mvn clean test

The Crawling tests cover:

- Gutenberg header/body splitting;
- datalake publication;
- READY state detection;
- idempotent repeated crawling;
- avoiding a second download when a book is already complete.

## Configuration

The Crawling role uses the Worker configuration.

Relevant environment variables include:

- WORKER_ROLES: must contain crawl for the worker to execute ingestion tasks.
- DATALAKE_DIR: shared datalake directory available to Crawling and Indexing workers.
- SEED_BOOKS: optional comma-separated list of Project Gutenberg book IDs used to create initial ingest tasks.
- TASK_MAX_ATTEMPTS: maximum number of attempts before a distributed task is marked as failed.

Example:

export WORKER_ROLES=crawl
export DATALAKE_DIR=/shared/skimsearch/datalake
export SEED_BOOKS=1342,11,84

## Stage 3 role in the distributed workflow

Distributed ingest task
        |
        v
Worker with crawl role
        |
        v
Project Gutenberg
        |
        v
Shared datalake
        |
        v
bookReady(bookId)
        |
        v
Distributed indexing task

The distributed queue, leases and task recovery are managed by the Worker
module. The Crawling module is responsible only for safely acquiring and
publishing the document data.