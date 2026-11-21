package com.skimsearch.crawl;

public final class CrawlingRole {

    private final BookDownloader downloader;
    private final DatalakeWriter writer;

    public CrawlingRole(
            BookDownloader downloader,
            DatalakeWriter writer
    ) {
        this.downloader = downloader;
        this.writer = writer;
    }

    public Result ingestBook(int bookId)
            throws Exception {

        DatalakeWriter.State current =
                writer.state(bookId);

        if (current == DatalakeWriter.State.READY) {
            return Result.ALREADY_READY;
        }

        GutenbergClient.BookContent content =
                downloader.download(bookId);

        boolean written =
                writer.write(bookId, content);

        return written
                ? Result.DOWNLOADED
                : Result.ALREADY_READY;
    }

    public enum Result {
        DOWNLOADED,
        ALREADY_READY
    }
}