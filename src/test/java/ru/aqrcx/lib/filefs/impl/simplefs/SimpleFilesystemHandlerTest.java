package ru.aqrcx.lib.filefs.impl.simplefs;

import org.junit.jupiter.api.AfterEach;
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

    private SimpleFilesystemHandler fsHandler;

    @TempDir
    Path tempDir;

    @AfterEach
    void cleanUpEach() throws IOException {
        fsHandler.detach();
        fsHandler = null;
    }

    @Test
    void should_correctly_init_fs_and_read_its_version() throws IOException {
        File emptyFile = tempDir.resolve("should_correctly_init_fs_and_read_its_version").toFile();
        fsHandler = SimpleFilesystemHandler.initNewFilesystemAsync(emptyFile).join();

        Long version = fsHandler.getVersion();
        assertEquals(SimpleFilesystemHandler.VERSION, version);
    }

    @Test
    void should_correctly_attach_fs_and_read_its_version() throws IOException, URISyntaxException {
        File fsFile = getFileFromResources("emptyFs");
        fsHandler = SimpleFilesystemHandler.attachExistingFilesystemAsync(fsFile).join();

        Long version = fsHandler.getVersion();
        assertEquals(SimpleFilesystemHandler.VERSION, version);
    }

    @Test
    void should_write_correct_amount_of_bytes_to_fs() throws URISyntaxException, IOException {
        File fsFile = tempDir.resolve("should_write_correct_amount_of_bytes_to_fs").toFile();
        fsHandler = SimpleFilesystemHandler.initNewFilesystemAsync(fsFile).join();
        long fsFileLenAfterInit = fsFile.length();

        File fileToWrite = getFileFromResources("6KbFileToWrite");
        long fileToWriteLen = fileToWrite.length();

        String fileName = "first file";
        FileInputStream source = new FileInputStream(fileToWrite);
        fsHandler.writeAsync(fileName, source, fileToWriteLen)
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
                + SimpleFilesystemHandler.FILE_NAME_SIZE_BYTES
                + fileName.getBytes(StandardCharsets.UTF_8).length
                + SimpleFilesystemHandler.FILE_SIZE_BYTES
                + fileToWriteLen;

        assertEquals(expectedFsFileSizeAfterWrite, fsFile.length());
        assertEquals(SimpleFilesystemHandler.VERSION_BYTES, fsHandler.getFileOffset(fileName));
    }

    private File getFileFromResources(String filename) throws URISyntaxException {
        URL resource = getClass().getClassLoader().getResource(filename);

        if (resource == null) {
            throw new IllegalArgumentException("File \"" + filename + "\" not found");
        }

        return new File(resource.toURI());
    }
}
