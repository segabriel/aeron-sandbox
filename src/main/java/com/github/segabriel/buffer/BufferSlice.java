package com.github.segabriel.buffer;

import static java.lang.System.getProperty;

import java.util.Arrays;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Buffer slice.
 *
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |  Free Mark      |                                             |
 *  +-----------------+---------------+-----------------------------+
 *  |                       Message Length                          |
 *  +---------------------------------------------------------------+
 *  |                          Content                              |
 *  +-----------------+---------------+-----------------------------+
 * </pre>
 */
public class BufferSlice {

  public static final boolean DEBUG = "true".equalsIgnoreCase(getProperty("buffer.slice.debug", "false"));

  public static final int FREE_MARK_FIELD_OFFSET = Byte.BYTES;
  public static final int NEXT_READ_FIELD_OFFSET = Integer.BYTES;
  public static final int HEADER_OFFSET = FREE_MARK_FIELD_OFFSET + NEXT_READ_FIELD_OFFSET;

  private final UnsafeBuffer underlying;
  private final int offset;
  private final int length;

  public BufferSlice(UnsafeBuffer underlying, int offset, int length) {
    this.underlying = underlying;
    this.offset = offset;
    this.length = length;
  }

  public void release() {
    underlying.putByteVolatile(offset, (byte) 0);
    debugPrint("release");
  }

  public int offset() {
    return offset + HEADER_OFFSET;
  }

  public int capacity() {
    return length - HEADER_OFFSET;
  }

  public BufferSlice putBytes(int index, ByteBuffer srcBuffer, int srcIndex, int length) {
    underlying.putBytes(offset() + index, srcBuffer, srcIndex, length);
    debugPrint("putBytes");
    return this;
  }

  public BufferSlice putBytes(int index, byte[] src) {
    underlying.putBytes(offset() + index, src);
    debugPrint("putBytes");
    return this;
  }

  public void getBytes(int index, ByteBuffer dstBuffer, int dstOffset, int length) {
    underlying.getBytes(offset() + index, dstBuffer, dstOffset, length);
  }

  @Override
  public String toString() {
    byte[] bytes = new byte[capacity()];
    underlying.getBytes(offset(), bytes, 0, capacity());
    return new String(bytes);
  }

  public void debugPrint(String operation) {
    if (DEBUG) {
      System.err.println(operation);

      for (int i = 0; i < underlying.capacity(); i++) {
        System.err.print(i % 10);
      }
      System.err.println();
      byte[] bytes = new byte[underlying.capacity()];
      underlying.getBytes(0, bytes);
      for (int i = 0; i < bytes.length; i++) {
        char element = (char) bytes[i];
        if ((element >= 'a' && element <= 'z') || (element >= 'A' && element <= 'Z')) {
          continue;
        }
        bytes[i] = '_';
      }
      System.err.println(new String(bytes));
    }
  }
}
