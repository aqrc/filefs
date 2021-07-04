package ru.aqrcx.lib.filefs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
     * @param filename Name which will be assigned to file inside filesystem
     * @param dataStream File data
     * @param sourceSize Length of file data in bytes
     * @return CompletableFuture which indicates the result of write
     */
    CompletableFuture<Void> writeAsync(String filename, InputStream dataStream, long sourceSize);

    /**
     * Method deletes the file with {@code filename}
     * from the filesystem.
     * @param filename File to delete
     * @return CompletableFuture which indicates the result of deletion
     */
    CompletableFuture<Void> deleteAsync(String filename);

    /**
     * Reads file with {@code filename} from the filesystem
     * and writes it in {@code destination} stream.
     * @param filename File to read from filesystem
     * @param destination Stream where file data will be written
     * @return CompletableFuture which indicates the result of read
     */
    CompletableFuture<Void> readAsync(String filename, OutputStream destination);

    /**
     * Syncs in-memory data with the filesystem,
     * then closes the file.
     *
     * @throws IOException If some I/O error occur
     */
    void detach() throws IOException;
}
