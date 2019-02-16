package com.github.segabriel.buffer;

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

  public static final int FREE_MARK_FIELD_OFFSET = Byte.BYTES;
  public static final int MSG_LENGTH_FIELD_OFFSET = Integer.BYTES;
  public static final int HEADER_OFFSET = FREE_MARK_FIELD_OFFSET + MSG_LENGTH_FIELD_OFFSET;

  private final UnsafeBuffer underlying;
  private final int offset;
  private final int length;

  public BufferSlice(UnsafeBuffer underlying, int offset, int length) {
    this.underlying = underlying;
    this.offset = offset;
    this.length = length;
  }

  public void release() {
    underlying.putByteVolatile(0, (byte) 0);
  }

  public int offset() {
    return offset + HEADER_OFFSET;
  }

  public int capacity() {
    return length - HEADER_OFFSET;
  }

  public void putBytes(int index, ByteBuffer srcBuffer, int srcIndex, int length) {
    underlying.putBytes(offset() + index, srcBuffer, srcIndex, length);
  }

  public int putStringUtf8(int index, String value) {
    return underlying.putStringUtf8(offset() + index, value);
  }

  public String getStringUtf8(int index, int length) {
    return underlying.getStringUtf8(offset() + index, length);
  }

  public void getBytes(int index, ByteBuffer dstBuffer, int dstOffset, int length) {
    underlying.getBytes(offset() + index, dstBuffer, dstOffset, length);
  }

  @Override
  public String toString() {
    return underlying.getStringUtf8(offset(), capacity());
  }
}
