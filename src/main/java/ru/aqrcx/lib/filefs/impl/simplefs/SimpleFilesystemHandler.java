package ru.aqrcx.lib.filefs.impl.simplefs;

import ru.aqrcx.lib.filefs.FilesystemHandler;
import ru.aqrcx.lib.filefs.impl.exception.FileFsException;
import ru.aqrcx.lib.filefs.internal.util.ByteUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
    final static int FLAGS_SIZE_BYTES = Integer.BYTES;

    private final RandomAccessFile fs;
    private final FileChannel channel;
    private final HashMap<String, Long> fileOffsetsCache;

    /**
     * Initializes a {@code SimpleFilesystemHandler}
     * with an already existing filesystem from a {@code file}.
     * Scans the whole file, caches offsets into {@code fileOffsetsCache}.
     *
     * @param file A valid and existing file
     * @throws IOException When {@code file} not found or other I/O error occurs
     */
    private SimpleFilesystemHandler(File file) throws IOException {
        this.fs = new RandomAccessFile(file, "rw");
        this.channel = fs.getChannel();
        this.channel.tryLock(); // TODO handle properly

        Long fsVersion = getVersion();
        if (!VERSION.equals(fsVersion)) {
            // TODO something for back compatibility, maybe a factory
        }
        this.fileOffsetsCache = getFileOffsets();
    }

    private HashMap<String, Long> getFileOffsets() throws IOException {
        HashMap<String, Long> fileOffsets = new HashMap<>();

        ByteBuffer flagsBuffer = ByteBuffer.allocate(FLAGS_SIZE_BYTES);
        ByteBuffer filenameSizeBuffer = ByteBuffer.allocate(FILE_NAME_SIZE_BYTES);
        ByteBuffer fileSizeBuffer = ByteBuffer.allocate(FILE_SIZE_BYTES);

        long nextFileOffset = VERSION_BYTES;

        while (nextFileOffset < channel.size()) {
            channel.position(nextFileOffset);

            readAndFlip(flagsBuffer);
            int flags = flagsBuffer.getInt();

            readAndFlip(filenameSizeBuffer);
            int filenameLen = filenameSizeBuffer.getInt();

            if (isFileDeleted(flags)) {
                channel.position(channel.position() + filenameLen);
                readAndFlip(fileSizeBuffer);
                long fileSize = fileSizeBuffer.getLong();
                nextFileOffset += getFilePropertiesSize(filenameLen) + fileSize;
                flagsBuffer.clear();
                filenameSizeBuffer.clear();
                fileSizeBuffer.clear();
                continue;
            }

            ByteBuffer filenameBuffer = ByteBuffer.allocate(filenameLen);
            readAndFlip(filenameBuffer);
            String filename = StandardCharsets.UTF_8.decode(filenameBuffer).toString();
            fileOffsets.put(filename, nextFileOffset);

            readAndFlip(fileSizeBuffer);
            nextFileOffset += getFilePropertiesSize(filenameLen) + fileSizeBuffer.getLong();

            flagsBuffer.clear();
            filenameSizeBuffer.clear();
            fileSizeBuffer.clear();
        }

        return fileOffsets;
    }

    private void readAndFlip(ByteBuffer buffer) throws IOException {
        channel.read(buffer);
        buffer.flip();
    }

    private boolean isFileDeleted(int flags) {
        return (flags >> 0 & 1) == 1;
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
    public static CompletableFuture<SimpleFilesystemHandler> initThenMountFilesystemAsync(File file) {
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
     * @param file An existing file which will contain the filesystem
     * @return Handler for the {@code file}'s filesystem
     * @throws IOException When {@code file} not found or other I/O error occurs
     */
    private static SimpleFilesystemHandler initFileSystem(File file) throws IOException {
        try (RandomAccessFile filesystem = new RandomAccessFile(file, "rw")) {
            try (FileChannel channel = filesystem.getChannel()) {
                channel.truncate(VERSION_BYTES);
                channel.write(ByteBuffer.allocateDirect(VERSION_BYTES));

                channel.position(0);
                channel.write(ByteUtils.longToBytes(VERSION));

                channel.force(true);
            }
        }

        return new SimpleFilesystemHandler(file);
    }

    /**
     * A method which opens the specified {@code file} as a filesystem.
     * Filesystem in a file must be initialized in a file beforehand.
     *
     * @param file An existing file which already contains a filesystem
     * @return CompletableFuture with a handler for the {@code file}'s filesystem
     *         or with an Exception if I/O error occurred
     */
    public static CompletableFuture<SimpleFilesystemHandler> mountExistingFilesystemAsync(File file) {
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
     * @return Version of this filesystem
     * @throws IOException If some I/O error occur
     */
    Long getVersion() throws IOException {
        channel.position(0);
        ByteBuffer versionBuffer = ByteBuffer.allocate(VERSION_BYTES);
        readAndFlip(versionBuffer);
        return versionBuffer.getLong();
    }

    /**
     * This method is needed mostly for testing,
     * to not expose cache itself.
     *
     * @param filename Name of the file
     * @return Offset of the file or null if such file
     *         is not found in the cache
     */
    Long getFileOffset(String filename) {
        return this.fileOffsetsCache.get(filename);
    }

    /**
     * @param filenameLength Length of filename in bytes
     * @return Length of properties before file data in bytes
     */
    static int getFilePropertiesSize(int filenameLength) {
        return FLAGS_SIZE_BYTES + FILE_NAME_SIZE_BYTES + filenameLength + FILE_SIZE_BYTES;
    }

    /**
     * Writes following data in the end of file-filesystem:
     *  - flags
     *  - length of {@code filename} in bytes (int, 4 bytes)
     *  - {@code filename}
     *  - length of file's data (long, 8 bytes)
     *  - file's content (from {@code source})
     *
     * @param filename Name which will be assigned to file inside filesystem
     * @param source File data
     * @param sourceSize Length of file data in bytes
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
        int filenameLen = filename.getBytes(StandardCharsets.UTF_8).length;
        int filePropertiesSize = getFilePropertiesSize(filenameLen);
        long fsLen;
        int flags = 0;

        ByteBuffer filePropertiesBuffer =
                ByteBuffer.allocate(filePropertiesSize)
                        .put(ByteUtils.intToBytes(flags))
                        .put(ByteUtils.intToBytes(filenameLen))
                        .put(filename.getBytes(StandardCharsets.UTF_8))
                        .put(ByteUtils.longToBytes(sourceSize));
        filePropertiesBuffer.flip();

        synchronized (this) {
            fsLen = channel.size();
            channel.position(fsLen);
            channel.write(filePropertiesBuffer);

            try (ReadableByteChannel sourceChannel = Channels.newChannel(source)) {
                channel.transferFrom(sourceChannel, channel.size(), sourceSize);
            }

            fileOffsetsCache.put(filename, fsLen);
        }
    }

    /**
     * Method marks the file with {@code filename} as deleted
     * and removes it from cache. Or does nothing if the file
     * with such {@code filename} doesn't exist in cache.
     * Eventually consistent.
     *
     * @param filename File to delete
     * @return CompletableFuture which indicates the result of delete
     *         (contains an Exception if I/O error occurred)
     */
    @Override
    public CompletableFuture<Void> deleteAsync(String filename) {
        return wrapInFuture((future) -> {
            try {
                delete(filename);
                future.complete(null);
            } catch (IOException e) {
                future.completeExceptionally(
                        new FileFsException("Exception occurred on file \"" + filename + "\" deletion", e));
            }
        });
    }

    private void delete(String filename) throws IOException {
        Long fileOffset = fileOffsetsCache.get(filename);

        if (fileOffset == null) {
            return;
        }

        ByteBuffer flagsBuffer = ByteUtils.intToBytes(1);

        synchronized (this) {
            channel.position(fileOffset);
            channel.write(flagsBuffer);
            fileOffsetsCache.remove(filename, fileOffset);
        }
    }

    /**
     * Method finds file in cache by {@code filename}
     * and writes it in {@code destination} stream.
     * If file with this name doesn't exist in the filesystem
     * just closes the stream.
     *
     * @param filename File to read from filesystem
     * @param destination Stream where file data will be written
     * @return CompletableFuture which indicates the result of read
     *         (contains an Exception if I/O error occurred)
     */
    @Override
    public CompletableFuture<Void> readAsync(String filename, OutputStream destination) {
        return wrapInFuture((future) -> {
            try {
                read(filename, destination);
                future.complete(null);
            } catch (IOException e) {
                future.completeExceptionally(
                        new FileFsException("Exception occurred on file \"" + filename + "\" read", e));
            }
        });
    }

    private void read(String filename, OutputStream destination) throws IOException {
        Long fileOffset = fileOffsetsCache.get(filename);

        if (fileOffset == null) {
            destination.close();
            return;
        }

        int filenameLen = filename.getBytes(StandardCharsets.UTF_8).length;
        ByteBuffer fileSizeBuffer = ByteBuffer.allocate(FILE_SIZE_BYTES);

        synchronized (this) {
            channel.position(fileOffset + FILE_NAME_SIZE_BYTES + filenameLen);
            channel.read(fileSizeBuffer);
        }
        fileSizeBuffer.flip();
        long fileSize = fileSizeBuffer.getLong();
        long fileDataOffset = fileOffset + getFilePropertiesSize(filenameLen);
        try (WritableByteChannel destinationChannel = Channels.newChannel(destination)) {
            channel.transferTo(fileDataOffset, fileSize, destinationChannel);
        }
        destination.close();
    }

    /**
     * Marks existing file with {@code filename} as deleted
     * and writes new one from {@code source}.
     *
     * @param filename Name which will be assigned to file inside filesystem
     * @param source File data
     * @param sourceSize Length of file data in bytes
     * @return CompletableFuture which indicates the result of update
     *         (contains an Exception if I/O error occurred)
     */
    @Override
    public CompletableFuture<Void> updateAsync(String filename, InputStream source, long sourceSize) {
        return wrapInFuture((future) -> {
            try {
                delete(filename);
                write(filename, source, sourceSize);
                future.complete(null);
            } catch (IOException e) {
                future.completeExceptionally(
                        new FileFsException("Exception occurred on file \"" + filename + "\" update", e));
            }
        });
    }

    /**
     * Search for filenames with {@code path} prefix
     * in the cache. If {@code path} is null or empty
     * returns all filenames.
     *
     * @param path The path of files to list
     * @return CompletableFuture with the set of filenames
     *         with {@code path} prefix
     */
    @Override
    public CompletableFuture<Set<String>> listAsync(String path) {
        if (path == null || path.isEmpty()) {
            return CompletableFuture.completedFuture(fileOffsetsCache.keySet());
        }

        return CompletableFuture.completedFuture(
                fileOffsetsCache.keySet().stream()
                        .filter(filename -> filename.startsWith(path))
                        .collect(Collectors.toSet())
        );
    }

    /**
     * Forces data to be written on storage device,
     * then closes the channel and the file.
     *
     * @throws IOException If some I/O error occur
     */
    @Override
    public void unmount() throws IOException {
        channel.force(true);
        channel.close();
        fs.close();
    }

    @Override
    protected void finalize() throws IOException {
        unmount();
    }

    private static <T> CompletableFuture<T> wrapInFuture(Consumer<CompletableFuture<T>> consumer) {
        CompletableFuture<T> result = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> consumer.accept(result));
        return result;
    }
}
