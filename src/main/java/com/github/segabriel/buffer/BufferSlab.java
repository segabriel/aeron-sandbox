package com.github.segabriel.buffer;

import static java.lang.System.getProperty;

import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.UnsafeBuffer;

public class BufferSlab {

  public static final boolean DEBUG =
      "true".equalsIgnoreCase(getProperty("buffer.slice.debug", "false"));

  private static final AtomicInteger COUNTER = new AtomicInteger();
  private static final byte OCCUPIED = (byte) '?';
  private static final byte RELEASED = (byte) 0;
  private static final int R0 = 0;

  private final int id = COUNTER.incrementAndGet();
  private final UnsafeBuffer underlying;

  private int w0;
  private int r;
  private int w;

  public BufferSlab(UnsafeBuffer underlying) {
    this.underlying = underlying;
    this.w0 = 0;
    this.w = 0;
    this.r = 0;
    this.underlying.putByte(w, (byte) 0);
    this.underlying.putInt(w + BufferSlice.FREE_MARK_FIELD_OFFSET, 0);
  }

  public BufferSlice allocate(int size) {
    final int fullLength = size + BufferSlice.HEADER_OFFSET;
    return allocate(fullLength, w0, r, w);
  }

  private BufferSlice allocate(final int fullLength, int w0, int r, int w) {
    int availableBytes = underlying.capacity() - w;
    if (availableBytes >= fullLength) {
      this.w = w + fullLength;
      if (w == w0) {
        this.w0 = this.w;
      }
      return slice(w, fullLength);
    }

    if (w == w0) {
      int r0 = R0;

      while (isReleased(r0)) {
        int nextOffset = nextReadOffset(r0);

        if (nextOffset == r0) {
          if (r0 > r) {
            this.r = r0;
          }
          return null;
        }

        if (nextOffset == w) {
          this.w0 = this.w = this.r = 0;
          availableBytes = underlying.capacity();
          if (availableBytes >= fullLength) {
            this.w0 = this.w = fullLength;
            return slice(0, fullLength);
          }
          return null;
        }

        availableBytes = r0 = nextOffset;
        if (availableBytes >= fullLength) {
          this.w0 = fullLength;
          if (r0 > r) {
            this.r = r0;
          }
          return slice(0, fullLength);
        }
      }

      if (r0 > r) {
        this.r = r0;
      }

      return null;

    } else {
      availableBytes = r - w0;
      if (availableBytes >= fullLength) {
        this.w0 = w0 + fullLength;
        return slice(w0, fullLength);
      }

      while (isReleased(r)) {
        int nextOffset = nextReadOffset(r);

        if (nextOffset == r) {
          this.r = r;
          return null;
        }

        if (nextOffset == w) {
          this.r = r = R0;
          this.w = w = this.w0 = w0;

          availableBytes = underlying.capacity() - w;
          if (availableBytes >= fullLength) {
            this.w0 = this.w = w + fullLength;
            return slice(w, fullLength);
          }

          if (w == 0) {
            return null;
          }

          while (isReleased(r)) {
            nextOffset = nextReadOffset(r);

            if (nextOffset == r) {
              this.r = r;
              return null;
            }

            if (nextOffset == w) {
              this.w0 = this.w = this.r = 0;
              availableBytes = underlying.capacity();
              if (availableBytes >= fullLength) {
                this.w0 = this.w = fullLength;
                return slice(0, fullLength);
              }
              return null;
            }

            availableBytes = r = nextOffset;
            if (availableBytes >= fullLength) {
              this.w0 = fullLength;
              this.r = r;
              return slice(0, fullLength);
            }
          }
          return null;
        }

        r = nextOffset;
        availableBytes = r - w0;
        if (availableBytes >= fullLength) {
          this.w0 = w0 + fullLength;
          this.r = r;
          return slice(w0, fullLength);
        }
      }

      return null;
    }
  }

  private int nextReadOffset(int currentReadOffset) {
    if (currentReadOffset == w) {
      return currentReadOffset;
    }
    return underlying.getInt(currentReadOffset + BufferSlice.FREE_MARK_FIELD_OFFSET);
  }

  private boolean isReleased(int offset) {
    return underlying.getByteVolatile(offset) == RELEASED;
  }

  private BufferSlice slice(int offset, int fullLength) {
    underlying.putByte(offset, OCCUPIED);
    int nextOffset = offset + fullLength;
    underlying.putInt(offset + BufferSlice.FREE_MARK_FIELD_OFFSET, nextOffset);
    BufferSlice slice = new BufferSlice(this, offset, fullLength);
    if (DEBUG) {
      System.err.println(
          debugString("slice") + "| s, offset=" + offset + ", nextOffset=" + nextOffset);
    }
    return slice;
  }

  void release(int offset) {
    underlying.putByteVolatile(offset, RELEASED);
    if (DEBUG) {
      System.err.println(
          debugString("release")
              + "| r, offset="
              + offset
              + ", nextOffset="
              + nextReadOffset(offset));
    }
  }

  void putBytes(int index, byte[] src) {
    underlying.putBytes(index, src);
    if (DEBUG) {
      System.err.println(debugString("write") + "| w, index=" + index + ", length=" + src.length);
    }
  }

  void getBytes(int index, byte[] dst) {
    underlying.getBytes(index, dst);
  }

  private String debugString(String operation) {
    byte[] bytes = new byte[underlying.capacity()];
    getBytes(0, bytes);
    for (int i = 0; i < bytes.length; i++) {
      char element = (char) bytes[i];
      if ((element >= 'a' && element <= 'z')
          || (element >= 'A' && element <= 'Z')
          || element == OCCUPIED) {
        continue;
      }
      bytes[i] = '_';
    }
    return id
        + "/"
        + operation.charAt(0)
        + "/"
        + new String(bytes)
        + ", w0="
        + w0
        + ", r="
        + r
        + ", w="
        + w;
  }
}
