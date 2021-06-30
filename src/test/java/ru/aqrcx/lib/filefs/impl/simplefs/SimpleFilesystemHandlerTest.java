package ru.aqrcx.lib.filefs.impl.simplefs;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class SimpleFilesystemHandlerTest {

    @TempDir
    Path tempDir;

    @Test
    void should_correctly_init_fs_and_read_its_version() throws IOException {
        File emptyFile = tempDir.resolve("should_correctly_init_fs_and_read_its_version").toFile();
        SimpleFilesystemHandler fsHandler = SimpleFilesystemHandler.initFileSystemAsync(emptyFile).join();

        Long version = fsHandler.getVersion();
        assertEquals(SimpleFilesystemHandler.VERSION, version);

        fsHandler.detach();
    }

    @Test
    void should_write_correct_amount_of_bytes_to_fs() throws URISyntaxException, IOException {
        File fsFile = tempDir.resolve("should_write_correct_amount_of_bytes_to_fs").toFile();
        SimpleFilesystemHandler fsHandler = SimpleFilesystemHandler.initFileSystemAsync(fsFile).join();
        long fsFileLenAfterInit = fsFile.length();

        File fileToWrite = getFileFromResources("6KbFileToWrite");
        long fileToWriteLen = fileToWrite.length();

        String fileName = "first file";
        FileInputStream source = new FileInputStream(fileToWrite);
        fsHandler.writeAsync(fileName, source)
                .thenAccept(unused -> {
                    try {
                        source.close();
                    } catch (IOException e) {
                        fail(e);
                    }
                })
                .exceptionally(Assertions::fail)
                .join();

        long expectedFsFileSizeAfterWrite = fsFileLenAfterInit
                + Integer.BYTES
                + fileName.getBytes(StandardCharsets.UTF_8).length
                + Long.BYTES
                + fileToWriteLen;

        assertEquals(expectedFsFileSizeAfterWrite, fsFile.length());

        fsHandler.detach();
    }

    private File getFileFromResources(String filename) throws URISyntaxException {
        URL resource = getClass().getClassLoader().getResource(filename);

        if (resource == null) {
            throw new IllegalArgumentException("File \"" + filename + "\" not found");
        }

        return new File(resource.toURI());
    }
}
