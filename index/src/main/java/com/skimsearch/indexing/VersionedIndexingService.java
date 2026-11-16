package com.skimsearch.indexing;

import com.skimsearch.common.IndexManifest;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the next index version from books in the datalake.
 * All books are read and tokenised first: if one is missing or incomplete,
 * the exception is thrown before any index file is written.
 */
public final class VersionedIndexingService {

    private final DatalakeReader datalake;
    private final IndexVersionWriter writer;

    public VersionedIndexingService(DatalakeReader datalake, IndexVersionWriter writer) {
        this.datalake = datalake;
        this.writer = writer;
    }

    /** New version = previous version + these books. Returns its manifest. */
    public IndexManifest indexBooks(List<Integer> bookIds) {
        List<IndexStore.BookIndexData> books = new ArrayList<>();
        for (int bookId : bookIds) {
            DatalakeReader.RawBook raw = datalake.read(bookId);
            books.add(new IndexStore.BookIndexData(
                    MetadataExtractor.extract(bookId, raw.header()),
                    Tokenizer.termFrequencies(raw.body())));
        }
        return writer.buildNextVersionWithManifest(books);
    }
}