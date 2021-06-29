package ru.aqrcx.lib.filefs.internal.util;

import java.nio.ByteBuffer;

/**
 * An util class created to deduplicate code
 * required to transform data types into bytes.
 *
 * FOR INTERNAL USE ONLY.
 * IMPLEMENTATION IS SUBJECT TO CHANGE.
 *
 * @see <a href="https://stackoverflow.com/questions/4485128/">stackoverflow</a>
 */
public class ByteUtils {
    public static ByteBuffer longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).putLong(x);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer intToBytes(int x) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).putInt(x);
        buffer.flip();
        return buffer;
    }
}