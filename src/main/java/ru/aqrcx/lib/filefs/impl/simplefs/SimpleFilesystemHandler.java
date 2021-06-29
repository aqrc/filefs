package ru.aqrcx.lib.filefs.impl.simplefs;

import ru.aqrcx.lib.filefs.FilesystemHandler;
import ru.aqrcx.lib.filefs.internal.util.ByteUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * <code>SimpleFilesystemHandler</code> is an implementation
 * of <code>FilesystemHandler</code> and is dummy simple.
 * TODO: explain principle
 */
public class SimpleFilesystemHandler implements FilesystemHandler {
    private final RandomAccessFile fs;
    private final static int VERSION_SIZE = Long.BYTES;
    public final static Long VERSION = 1L;

    /**
     * Initializes a {@code SimpleFilesystemHandler}
     * with an already existing filesystem from a {@code file}.
     * @param file A valid and existing file
     * @throws IOException When {@code file} not found or other I/O error occurs
     */
    public SimpleFilesystemHandler(File file) throws IOException {
        this.fs = new RandomAccessFile(file, "rws");

        Long fsVersion = getVersion();
        if (!VERSION.equals(fsVersion)) {
            // TODO something for back compatibility or a factory
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
     * @return Handler for the {@code file}'s filesystem
     * @throws IOException When {@code file} not found or other I/O error occurs
     */
    public static SimpleFilesystemHandler initFileSystem(File file) throws IOException {
        RandomAccessFile filesystem = new RandomAccessFile(file, "rw");
        FileChannel channel = filesystem.getChannel();

        channel.truncate(VERSION_SIZE);
        channel.write(ByteBuffer.allocateDirect(VERSION_SIZE));

        channel.position(0);
        channel.write(ByteUtils.longToBytes(VERSION));

        channel.force(true);
        channel.close();
        filesystem.close();

        return new SimpleFilesystemHandler(file);
    }

    /**
     * @return superblock of this filesystem
     * @throws IOException If some I/O error occur
     */
    Long getVersion() throws IOException {
        fs.seek(0);
        return fs.readLong();
    }

    @Override
    protected void finalize() throws IOException {
        fs.close();
    }
}
