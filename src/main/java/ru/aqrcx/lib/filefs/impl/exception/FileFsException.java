package ru.aqrcx.lib.filefs.impl.exception;

import java.io.IOException;

public class FileFsException extends IOException {
    public FileFsException(String message, Throwable cause) {
        super(message, cause);
    }
}
