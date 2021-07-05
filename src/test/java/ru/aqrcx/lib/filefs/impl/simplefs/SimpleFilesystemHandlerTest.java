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
import java.util.LinkedHashMap;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleFilesystemHandlerTest {

    private SimpleFilesystemHandler fsHandler;

    @TempDir
    Path tempDir;

    @AfterEach
    void cleanUpEach() throws IOException {
        fsHandler.unmount();
        fsHandler = null;
    }

    @Test
    void should_correctly_init_fs_and_read_its_version() throws IOException {
        initEmptyFs("should_correctly_init_fs_and_read_its_version");

        Long version = fsHandler.getVersion();
        assertEquals(SimpleFilesystemHandler.VERSION, version);
    }

    @Test
    void should_correctly_mount_fs_and_read_its_version() throws IOException, URISyntaxException {
        File fsFile = getFileFromResources("emptyFs");
        fsHandler = SimpleFilesystemHandler.mountExistingFilesystemAsync(fsFile).join();

        Long version = fsHandler.getVersion();
        assertEquals(SimpleFilesystemHandler.VERSION, version);
    }

    @Test
    void should_write_correct_amount_of_bytes_to_fs() throws URISyntaxException {
        File fsFile = tempDir.resolve("should_write_correct_amount_of_bytes_to_fs").toFile();
        fsHandler = SimpleFilesystemHandler.initThenMountFilesystemAsync(fsFile).join();
        long fsFileLenAfterInit = fsFile.length();

        File fileToWrite = getFileFromResources("6KbFileToWrite");
        long fileToWriteLen = fileToWrite.length();

        String fileName = "first file";
        writeFileInFs(fileName, fileToWrite, fileToWriteLen);

        long expectedFsFileSizeAfterWrite = fsFileLenAfterInit
                + SimpleFilesystemHandler.getFilePropertiesSize(getUTF8StringLengthInBytes(fileName))
                + fileToWriteLen;

        assertEquals(expectedFsFileSizeAfterWrite, fsFile.length());
        assertEquals(SimpleFilesystemHandler.VERSION_BYTES, fsHandler.getFileOffset(fileName));
    }

    @Test
    void should_write_to_fs_then_read() throws URISyntaxException, IOException {
        initEmptyFs("should_write_to_fs_then_read_it-FS");

        File fileToWrite = getFileFromResources("6KbFileToWrite");
        long fileToWriteLen = fileToWrite.length();

        String fileName = "first file";
        writeFileInFs(fileName, fileToWrite, fileToWriteLen);

        File destinationFile = tempDir.resolve("should_write_to_fs_then_read_it-DEST").toFile();
        try (FileOutputStream destination = new FileOutputStream(destinationFile)) {
            fsHandler.readAsync(fileName, destination)
                    .exceptionally(Assertions::fail)
                    .join();
        }

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
    void should_write_to_fs_then_delete() throws URISyntaxException {
        initEmptyFs("should_write_to_fs_then_delete_it");

        File fileToWrite = getFileFromResources("6KbFileToWrite");
        long fileToWriteLen = fileToWrite.length();

        String fileName = "first file";
        writeFileInFs(fileName, fileToWrite, fileToWriteLen);

        assertNotNull(fsHandler.getFileOffset(fileName));

        fsHandler.deleteAsync(fileName)
                .exceptionally(Assertions::fail)
                .join();

       assertNull(fsHandler.getFileOffset(fileName));
    }

    @Test
    void should_write_to_fs_then_update() throws URISyntaxException, IOException {
        File fsFile = tempDir.resolve("should_write_correct_amount_of_bytes_to_fs").toFile();
        fsHandler = SimpleFilesystemHandler.initThenMountFilesystemAsync(fsFile).join();
        long fsFileLenAfterInit = fsFile.length();

        File fileToWrite = getFileFromResources("6KbFileToWrite");
        long fileToWriteLen = fileToWrite.length();

        String fileName = "first file";
        int fileNameLen = getUTF8StringLengthInBytes(fileName);

        writeFileInFs(fileName, fileToWrite, fileToWriteLen);
        long writeOffset = fsHandler.getFileOffset(fileName);

        try(FileInputStream updateSource = new FileInputStream(fileToWrite)){
            fsHandler.updateAsync(fileName, updateSource, fileToWriteLen)
                    .exceptionally(Assertions::fail)
                    .join();
        }

        long updateOffset = fsHandler.getFileOffset(fileName);

        long expectedDiff = SimpleFilesystemHandler.getFilePropertiesSize(fileNameLen) + fileToWriteLen;
        assertEquals(expectedDiff, updateOffset - writeOffset);

        long expectedFsFileSize = fsFileLenAfterInit
                + 2L * SimpleFilesystemHandler.getFilePropertiesSize(fileNameLen)
                + 2L * fileToWriteLen;
        assertEquals(expectedFsFileSize, fsFile.length());
    }

    @Test
    void should_write_several_files_to_fs_then_correctly_mount_it() throws IOException {
        File fsFile = tempDir.resolve("should_write_several_files_to_fs_then_correctly_mount_it").toFile();
        fsHandler = SimpleFilesystemHandler.initThenMountFilesystemAsync(fsFile).join();

        LinkedHashMap<String, String> filenamesToContents = new LinkedHashMap<String, String>() {{
            put("first file", "this is the first file in this filesystem");
            put("second-file", "will be deleted");
            put("/third_file", "third file,\nit contains\n3 lines");
            put("/fourth/file", "just to be sure");
        }};
        filenamesToContents.forEach(this::writeStringInFs);
        fsHandler.deleteAsync("second-file").join();

        fsHandler.unmount();

        fsHandler = SimpleFilesystemHandler.mountExistingFilesystemAsync(fsFile).join();

        Set<String> allFilenames = fsHandler.listAsync("").join();
        assertEquals(3, allFilenames.size());
        assertTrue(allFilenames.contains("first file"));
        assertTrue(allFilenames.contains("/third_file"));
        assertTrue(allFilenames.contains("/fourth/file"));

        Set<String> rootFilenames = fsHandler.listAsync("/").join();
        assertEquals(2, rootFilenames.size());
        assertTrue(rootFilenames.contains("/third_file"));
        assertTrue(rootFilenames.contains("/fourth/file"));
    }

    private void initEmptyFs(String should_write_to_fs_then_delete_it) {
        File fsFile = tempDir.resolve(should_write_to_fs_then_delete_it).toFile();
        fsHandler = SimpleFilesystemHandler.initThenMountFilesystemAsync(fsFile).join();
    }

    private void writeFileInFs(String fileName, File fileToWrite, long fileToWriteLen) {
        try (FileInputStream source = new FileInputStream(fileToWrite)) {
            fsHandler.writeAsync(fileName, source, fileToWriteLen)
                    .exceptionally(Assertions::fail)
                    .join();
        } catch (IOException e) {
            fail(e);
        }
    }

    private void writeStringInFs(String fileName, String data) {
        long fileToWriteLen = getUTF8StringLengthInBytes(data);
        try (ByteArrayInputStream source = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))) {
            fsHandler.writeAsync(fileName, source, fileToWriteLen)
                    .exceptionally(Assertions::fail)
                    .join();
        } catch (IOException e) {
            fail(e);
        }
    }

    private int getUTF8StringLengthInBytes(String data) {
        return data.getBytes(StandardCharsets.UTF_8).length;
    }


    private File getFileFromResources(String filename) throws URISyntaxException {
        URL resource = getClass().getClassLoader().getResource(filename);

        if (resource == null) {
            throw new IllegalArgumentException("File \"" + filename + "\" not found");
        }

        return new File(resource.toURI());
    }
}
