package ru.aqrcx.lib.filefs.impl.simplefs;

import ru.aqrcx.lib.filefs.FilesystemHandler;
import ru.aqrcx.lib.filefs.FilesystemProxy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Simplest implementation of {@code FilesystemProxy},
 * which straight-forward proxy requests to its FilesystemHandler.
 */
public class SimpleFilesystemProxy implements FilesystemProxy {
    private final FilesystemHandler filesystemHandler;

    public SimpleFilesystemProxy(FilesystemHandler filesystemHandler) {
        this.filesystemHandler = filesystemHandler;
    }

    @Override
    public CompletableFuture<Void> writeAsync(String filename, InputStream source, long sourceSize) {
        return filesystemHandler.writeAsync(filename, source, sourceSize);
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String filename) {
        return filesystemHandler.deleteAsync(filename);
    }

    @Override
    public CompletableFuture<Void> readAsync(String filename, OutputStream destination) {
        return filesystemHandler.readAsync(filename, destination);
    }

    @Override
    public CompletableFuture<Void> updateAsync(String filename, InputStream source, long sourceSize) {
        return filesystemHandler.updateAsync(filename, source, sourceSize);
    }

    @Override
    public CompletableFuture<Set<String>> listAsync(String path) {
        return filesystemHandler.listAsync(path);
    }

    @Override
    public CompletableFuture<Void> defrag() {
        return filesystemHandler.defrag();
    }

    @Override
    public void unmount() throws IOException {
        filesystemHandler.unmount();
    }

    @Override
    protected void finalize() throws IOException {
        unmount();
    }
}
