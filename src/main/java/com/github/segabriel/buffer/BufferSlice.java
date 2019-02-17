package com.github.segabriel.buffer;

import static java.lang.System.getProperty;

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

  public static final boolean DEBUG =
      "true".equalsIgnoreCase(getProperty("buffer.slice.debug", "false"));

  public static final int FREE_MARK_FIELD_OFFSET = Byte.BYTES;
  public static final int NEXT_READ_FIELD_OFFSET = Integer.BYTES;
  public static final int HEADER_OFFSET = FREE_MARK_FIELD_OFFSET + NEXT_READ_FIELD_OFFSET;

  private final BufferSlab slab;
  private final int offset;
  private final int length;

  public BufferSlice(BufferSlab slab, int offset, int length) {
    this.slab = slab;
    this.offset = offset;
    this.length = length;
  }

  public void release() {
    slab.release(offset);
    debugPrint("release");
  }

  public int offset() {
    return offset + HEADER_OFFSET;
  }

  public int capacity() {
    return length - HEADER_OFFSET;
  }

  public BufferSlice putBytes(int index, ByteBuffer srcBuffer, int srcIndex, int length) {
    slab.underlying.putBytes(offset() + index, srcBuffer, srcIndex, length);
    debugPrint("putBytes");
    return this;
  }

  public BufferSlice putBytes(int index, byte[] src) {
    slab.underlying.putBytes(offset() + index, src);
    debugPrint("putBytes");
    return this;
  }

  public void getBytes(int index, ByteBuffer dstBuffer, int dstOffset, int length) {
    slab.underlying.getBytes(offset() + index, dstBuffer, dstOffset, length);
  }

  public void getBytes(final int index, final byte[] dst) {
    slab.underlying.getBytes(offset() + index, dst);
  }

  @Override
  public String toString() {
    byte[] bytes = new byte[capacity()];
    slab.underlying.getBytes(offset(), bytes, 0, capacity());
    return new String(bytes);
  }

  private String liner;

  public void debugPrint(String operation) {
    if (DEBUG) {

      StringBuilder builder =
          new StringBuilder(slab.underlying.capacity() + 100).append(slab.id).append("/").append(operation.charAt(0) + "/")
//              .append("\n")
          ;

//      for (int i = 0; i < underlying.capacity(); i++) {
//        System.err.print(i % 10);
//      }
//      System.err.println();
      byte[] bytes = new byte[slab.underlying.capacity()];
      slab.underlying.getBytes(0, bytes);
      for (int i = 0; i < bytes.length; i++) {
        char element = (char) bytes[i];
        if ((element >= 'a' && element <= 'z') || (element >= 'A' && element <= 'Z') || element == '?') {
          continue;
        }
        bytes[i] = '_';
      }

      System.err.println(builder.append(new String(bytes)).append("r=").append(slab.readIndex).append(", w=").append(slab.writeIndex));
    }
  }
}
