package ru.aqrcx.lib.filefs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
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
     * @param source File data
     * @param sourceSize Length of file data in bytes
     * @return CompletableFuture which indicates the result of write
     */
    CompletableFuture<Void> writeAsync(String filename, InputStream source, long sourceSize);

    /**
     * Method deletes the file with {@code filename}
     * from the filesystem.
     *
     * @param filename File to delete
     * @return CompletableFuture which indicates the result of deletion
     */
    CompletableFuture<Void> deleteAsync(String filename);

    /**
     * Reads file with {@code filename} from the filesystem
     * and writes it in {@code destination} stream.
     *
     * @param filename File to read from filesystem
     * @param destination Stream where file data will be written
     * @return CompletableFuture which indicates the result of read
     */
    CompletableFuture<Void> readAsync(String filename, OutputStream destination);

    /**
     * Updates file with {@code filename} existing
     * in the filesystem with data from {@code stream}.
     * Exact behavior depends on implementation.
     *
     * @param filename Name which will be assigned to file inside filesystem
     * @param source File data
     * @param sourceSize Length of file data in bytes
     * @return CompletableFuture which indicates the result of update
     */
    CompletableFuture<Void> updateAsync(String filename, InputStream source, long sourceSize);

    /**
     * Lists files from the specified {@code path}.
     *
     * @param path The path of files to list
     * @return CompletableFuture with the set of filenames
     *         found in the path
     */
    CompletableFuture<Set<String>> listAsync(String path);

    /**
     * Method which performs defragmentation of filesystem.
     *
     * @return CompletableFuture which indicates
     *         the result of defragmentation
     */
    CompletableFuture<Void> defrag();

    /**
     * Syncs in-memory data with the filesystem,
     * then closes the filesystem.
     *
     * @throws IOException If some I/O error occur
     */
    void unmount() throws IOException;
}
