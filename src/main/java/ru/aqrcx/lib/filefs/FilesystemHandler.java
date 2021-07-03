package ru.aqrcx.lib.filefs;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

/**
 * The <code>FileSystemHandler</code> interface provides
 * for using a file as a filesystem.
 *
 * Implementations should focus on working
 * with the filesystem inside a file and remain low-level.
 */
public interface FilesystemHandler {
    /**
     * Creates a new file from {@code stream}
     * with {@code filename} in the filesystem.
     *
     * @param filename Name which will be assigned to the file inside filesystem
     * @param dataStream File data
     * @throws IOException If some I/O error occur
     */
    CompletableFuture<Void> writeAsync(String filename, InputStream dataStream, long sourceSize) throws IOException;

    /**
     * Syncs in-memory data with the filesystem,
     * then closes the file.
     *
     * @throws IOException If some I/O error occur
     */
    void detach() throws IOException;
}
