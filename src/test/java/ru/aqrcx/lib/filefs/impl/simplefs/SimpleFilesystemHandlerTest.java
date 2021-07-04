package ru.aqrcx.lib.filefs.impl.simplefs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

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
        initEmptyFs("should_correctly_init_fs_and_read_its_version");

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
        writeFileInFs(fileToWrite, fileToWriteLen, fileName);

        long expectedFsFileSizeAfterWrite = fsFileLenAfterInit
                + SimpleFilesystemHandler.getFilePropertiesSize(fileName.getBytes(StandardCharsets.UTF_8).length)
                + fileToWriteLen;

        assertEquals(expectedFsFileSizeAfterWrite, fsFile.length());
        assertEquals(SimpleFilesystemHandler.VERSION_BYTES, fsHandler.getFileOffset(fileName));
    }

    @Test
    void should_write_to_fs_then_read_it() throws URISyntaxException, IOException {
        initEmptyFs("should_write_to_fs_then_read_it-FS");

        File fileToWrite = getFileFromResources("6KbFileToWrite");
        long fileToWriteLen = fileToWrite.length();

        String fileName = "first file";
        writeFileInFs(fileToWrite, fileToWriteLen, fileName);

        File destinationFile = tempDir.resolve("should_write_to_fs_then_read_it-DEST").toFile();
        FileOutputStream destination = new FileOutputStream(destinationFile);

        fsHandler.readAsync(fileName, destination)
                .thenAccept(unused -> {
                    try {
                        destination.close();
                    } catch (IOException e) {
                        fail(e);
                    }
                })
                .exceptionally(Assertions::fail)
                .join();

        try (BufferedReader originalFileReader = new BufferedReader(new FileReader(fileToWrite))) {
            try (BufferedReader destinationFileReader = new BufferedReader(new FileReader(destinationFile))) {
                String originalLine;
                String destLine;
                while ((originalLine = originalFileReader.readLine()) != null) {
                    destLine = destinationFileReader.readLine();
                    assertEquals(originalLine, destLine);
                }
                assertNull(destinationFileReader.readLine());
            }
        }
    }

    @Test
    void should_write_to_fs_then_delete_it() throws URISyntaxException, IOException {
        initEmptyFs("should_write_to_fs_then_delete_it");

        File fileToWrite = getFileFromResources("6KbFileToWrite");
        long fileToWriteLen = fileToWrite.length();

        String fileName = "first file";
        writeFileInFs(fileToWrite, fileToWriteLen, fileName);

        assertNotNull(fsHandler.getFileOffset(fileName));

        fsHandler.deleteAsync(fileName)
                .exceptionally(Assertions::fail)
                .join();

       assertNull(fsHandler.getFileOffset(fileName));
    }

    private void initEmptyFs(String should_write_to_fs_then_delete_it) {
        File fsFile = tempDir.resolve(should_write_to_fs_then_delete_it).toFile();
        fsHandler = SimpleFilesystemHandler.initNewFilesystemAsync(fsFile).join();
    }

    private void writeFileInFs(File fileToWrite, long fileToWriteLen, String fileName) throws FileNotFoundException {
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
    }

    private File getFileFromResources(String filename) throws URISyntaxException {
        URL resource = getClass().getClassLoader().getResource(filename);

        if (resource == null) {
            throw new IllegalArgumentException("File \"" + filename + "\" not found");
        }

        return new File(resource.toURI());
    }
}
