package com.github.segabriel.buffer;

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

  static final int FREE_MARK_FIELD_OFFSET = Byte.BYTES;
  static final int NEXT_READ_FIELD_OFFSET = Integer.BYTES;
  static final int HEADER_OFFSET = FREE_MARK_FIELD_OFFSET + NEXT_READ_FIELD_OFFSET;

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
  }

  public int offset() {
    return offset + HEADER_OFFSET;
  }

  public int capacity() {
    return length - HEADER_OFFSET;
  }

  public BufferSlice putBytes(int index, byte[] src) {
    slab.putBytes(offset() + index, src);
    return this;
  }

  public void getBytes(final int index, final byte[] dst) {
    slab.getBytes(offset() + index, dst);
  }

  @Override
  public String toString() {
    byte[] bytes = new byte[capacity()];
    slab.getBytes(offset(), bytes);
    return new String(bytes);
  }
}
