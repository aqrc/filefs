package ru.aqrcx.lib.filefs.impl.simplefs;

import ru.aqrcx.lib.filefs.FilesystemHandler;
import ru.aqrcx.lib.filefs.impl.exception.FileFsException;
import ru.aqrcx.lib.filefs.internal.util.ByteUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * <code>SimpleFilesystemHandler</code> is an implementation
 * of <code>FilesystemHandler</code> and is dummy simple.
 * TODO: explain principle
 */
public class SimpleFilesystemHandler implements FilesystemHandler {
    public final static Long VERSION = 1L;
    final static int VERSION_BYTES = Long.BYTES;
    final static int FILE_SIZE_BYTES = Long.BYTES;
    final static int FILE_NAME_SIZE_BYTES = Integer.BYTES;

    private final RandomAccessFile fs;
    private final FileChannel channel;
    /**
     * Initializes a {@code SimpleFilesystemHandler}
     * with an already existing filesystem from a {@code file}.
     * @param file A valid and existing file
     * @throws IOException When {@code file} not found or other I/O error occurs
     */
    private SimpleFilesystemHandler(File file) throws IOException {
        this.fs = new RandomAccessFile(file, "rws");
        this.channel = fs.getChannel();

        Long fsVersion = getVersion();
        if (!VERSION.equals(fsVersion)) {
            // TODO something for back compatibility, maybe a factory
        }
    }

    /**
     * A method which should be used to create a new filesystem
     * in a specified {@code file}. Creation includes zeroing
     * file's content, writing version.
     *
     * IMPORTANT: previous content of the {@code file} will be overridden.
     *
     * @param file An existing file which will contain the filesystem
     * @return CompletableFuture with a handler for the {@code file}'s filesystem
     *         or with an Exception if I/O error occurred
     */
    public static CompletableFuture<SimpleFilesystemHandler> initNewFilesystemAsync(File file) {
        return wrapInFuture((future) -> {
            try {
                future.complete(initFileSystem(file));
            } catch (IOException e) {
                future.completeExceptionally(
                        new FileFsException("Exception occurred on FS init", e));
            }
        });
    }


    /**
     * A method which opens the specified {@code file} as a filesystem.
     * Filesystem in a file must be initialized in a file beforehand.
     * @param file An existing file which already contains a filesystem
     * @return CompletableFuture with a handler for the {@code file}'s filesystem
     *         or with an Exception if I/O error occurred
     */
    public static CompletableFuture<SimpleFilesystemHandler> attachExistingFilesystemAsync(File file) {
        return wrapInFuture((future) -> {
            try {
                future.complete(new SimpleFilesystemHandler(file));
            } catch (IOException e) {
                future.completeExceptionally(
                        new FileFsException("Exception occurred on FS attach", e));
            }
        });
    }

    /**
     * @param file An existing file which will contain the filesystem
     * @return Handler for the {@code file}'s filesystem
     * @throws IOException When {@code file} not found or other I/O error occurs
     */
    private static SimpleFilesystemHandler initFileSystem(File file) throws IOException {
        RandomAccessFile filesystem = new RandomAccessFile(file, "rw");
        FileChannel channel = filesystem.getChannel();

        channel.truncate(VERSION_BYTES);
        channel.write(ByteBuffer.allocateDirect(VERSION_BYTES));

        channel.position(0);
        channel.write(ByteUtils.longToBytes(VERSION));

        channel.force(true);
        channel.close();
        filesystem.close();

        return new SimpleFilesystemHandler(file);
    }

    /**
     * @return Version of this filesystem
     * @throws IOException If some I/O error occur
     */
    Long getVersion() throws IOException {
        fs.seek(0);
        return fs.readLong();
    }

    /**
     * Writes following data in the end of file-filesystem:
     *  - length of {@code filename} in bytes (int, 4 bytes)
     *  - {@code filename}
     *  - length of file's data (long, 8 bytes)
     *  - file's content (from {@code source})
     *
     * @param filename Name which will be assigned to file inside filesystem
     * @param source File data
     * @return CompletableFuture which indicates the result of write
     *         (contains an Exception if I/O error occurred)
     */
    @Override
    public CompletableFuture<Void> writeAsync(String filename, InputStream source, long sourceSize){
        return wrapInFuture((future) -> {
            try {
                write(filename, source, sourceSize);
                future.complete(null);
            } catch (IOException e) {
                future.completeExceptionally(
                        new FileFsException("Exception occurred on file \"" + filename + "\" write", e));
            }
        });
    }

    private void write(String filename, InputStream source, long sourceSize) throws IOException {
        int filenameLen = filename.length();
        int filePropertiesSize = FILE_NAME_SIZE_BYTES + filenameLen + FILE_SIZE_BYTES;

        synchronized (this) {
            long fsLen = channel.size();
            FileLock lock = channel.lock(fsLen, filePropertiesSize + sourceSize, false);
            channel.position(fsLen);

            ByteBuffer filePropertiesBuffer =
                    ByteBuffer.allocate(filePropertiesSize)
                            .put(ByteUtils.intToBytes(filenameLen))
                            .put(filename.getBytes(StandardCharsets.UTF_8))
                            .put(ByteUtils.longToBytes(sourceSize));
            filePropertiesBuffer.flip();
            channel.write(filePropertiesBuffer);

            ReadableByteChannel sourceChannel = Channels.newChannel(source);
            channel.transferFrom(sourceChannel, channel.size(), sourceSize);

            lock.release();
            sourceChannel.close();
        }
    }

    @Override
    public void detach() throws IOException {
        channel.force(true);
        channel.close();
        fs.close();
    }

    @Override
    protected void finalize() throws IOException {
        detach();
    }

    private static <T> CompletableFuture<T> wrapInFuture(Consumer<CompletableFuture<T>> consumer) {
        CompletableFuture<T> result = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> consumer.accept(result));
        return result;
    }
}
