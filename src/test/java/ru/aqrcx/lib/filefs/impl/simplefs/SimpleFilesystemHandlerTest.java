package ru.aqrcx.lib.filefs.impl.simplefs;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleFilesystemHandlerTest {

    @ParameterizedTest
    @ValueSource(strings = {"emptyFile", "6KbFile"})
    void should_correctly_init_fs_and_read_its_version(String filename) throws URISyntaxException, IOException {
        URL resource = getClass().getClassLoader().getResource(filename);

        if (resource == null) {
            throw new IllegalArgumentException("File \"" + filename + "\" not found"); // should not happen
        }

        File testFile = new File(resource.toURI());
        SimpleFilesystemHandler fileHandler = SimpleFilesystemHandler.initFileSystem(testFile);

        Long version = fileHandler.getVersion();
        assertEquals(SimpleFilesystemHandler.VERSION, version);
    }
}
