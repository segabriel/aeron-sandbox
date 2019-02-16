package com.github.segabriel.buffer;

import org.agrona.concurrent.UnsafeBuffer;

public class BufferSlab {

  private final UnsafeBuffer underlying;

  private int readIndex;
  private int writeIndex;

  public BufferSlab(UnsafeBuffer underlying) {
    this.underlying = underlying;
    reset();
  }

  public BufferSlice allocate(int size /*without headers*/) {
    final int fullLength = size + BufferSlice.HEADER_OFFSET;
    return allocate(fullLength, writeIndex, readIndex);
  }

  private BufferSlice allocate(final int fullLength, int wIndex, int rIndex) {

    if (rIndex > wIndex) {
      // not enough => move rIndex until not enough and then if not enough again => null
      //  ---w-----r--
      //  ---w------r-
      //  ---w-------r
      //  r--w--------
      //  -r-w--------
      //  --rw--------
      //  ---b--------

      //  w--------r--
      //  w---------r-
      //  w----------r
      //  b-----------

      int availableBytes = rIndex - wIndex;
      if (availableBytes >= fullLength) {
        this.writeIndex = wIndex + fullLength;
        this.readIndex = rIndex;
        return slice(wIndex, fullLength);
      }

      if (!isReleased(rIndex)) {
        return null;
      }

      while (isReleased(rIndex)) {
        int nextOffset = nextOffset(rIndex);
        if (nextOffset < 0) {
          rIndex = 0;
          continue;
        }
        if (nextOffset == this.writeIndex) {
          // whole buffer is available, reset all index
          wIndex = 0;
          reset();
          availableBytes = underlying.capacity();
          if (availableBytes >= fullLength) {
            this.writeIndex = wIndex + fullLength;
            return slice(wIndex, fullLength);
          }
          return null;
        }
        rIndex = nextOffset;

        if (rIndex > wIndex) {
          availableBytes = rIndex - wIndex;
        }
        if (wIndex > rIndex) {
          availableBytes = underlying.capacity() - wIndex;
        }

        if (availableBytes >= fullLength) {
          this.writeIndex = wIndex + fullLength;
          this.readIndex = rIndex;
          if (this.writeIndex == underlying.capacity()) {
            this.writeIndex = 0;
          }
          return slice(wIndex, fullLength);
        }
      }

      this.readIndex = rIndex;
      return null;
    }

    if (wIndex > rIndex) {
      // not enough => change wIndex = 0, try again and if not enough again => null
      //  ------r---w--
      //  ------r----w-
      //  ------r-----w
      //  w-----r------

      //  r---------w--
      //  r----------w-
      //  r-----------w
      //  b------------

      int availableBytes = underlying.capacity() - wIndex;
      if (availableBytes >= fullLength) {
        this.writeIndex = wIndex + fullLength;
        this.readIndex = rIndex;
        if (this.writeIndex == underlying.capacity()) {
          this.writeIndex = 0;
        }
        return slice(wIndex, fullLength);
      }

      wIndex = 0;
      return allocate(fullLength, wIndex, rIndex);
    }

    if (isReleased(rIndex)) { // rIndex == wIndex
      int nextOffset = nextOffset(rIndex);
      if (nextOffset == rIndex) {
        // whole buffer is available
        if (rIndex != 0) {
          wIndex = 0;
          rIndex = 0;
          reset();
        }
        int availableBytes = underlying.capacity();
        if (availableBytes >= fullLength) {
          this.writeIndex = wIndex + fullLength;
          return slice(wIndex, fullLength);
        }
        return null;
      }

      if (nextOffset < 1) {
        return null;
      }
      rIndex = nextOffset;
      return allocate(fullLength, wIndex, rIndex);
    }

    this.readIndex = rIndex;
    return null;
  }

  /**
   * NOTE only for readIndex!!!
   *
   * <p>it will return the same offset if the next offset equals underlying.capacity()
   *
   * <p>it will return readIndex if the next offset equals readIndex
   *
   * <p>it will return -1 if the next offset exceeds underlying.capacity()
   *
   * @param offset current offset
   * @return next offset
   */
  private int nextOffset(int offset) {
    int offsetAndHeaders = offset + BufferSlice.HEADER_OFFSET;
    if (offsetAndHeaders >= readIndex) {
      return readIndex;
    }
    if (offsetAndHeaders >= underlying.capacity()) {
      return -1;
    }

    int msgLength = underlying.getInt(offset + BufferSlice.FREE_MARK_FIELD_OFFSET);
    int nextOffset = offsetAndHeaders + msgLength;
    return nextOffset == underlying.capacity() ? offset : nextOffset;
  }

  private boolean isReleased(int offset) {
    return underlying.getByte(offset) == 0;
  }

  private BufferSlice slice(int offset, int fullLength) {
    underlying.putByte(offset, (byte) 1);
    int msgLength = fullLength - BufferSlice.HEADER_OFFSET;
    underlying.putInt(offset + BufferSlice.FREE_MARK_FIELD_OFFSET, msgLength);
    return new BufferSlice(underlying, offset, fullLength);
  }

  private void reset() {
    this.writeIndex = 0;
    this.readIndex = 0;
    underlying.putByteVolatile(writeIndex, (byte) 0);
    underlying.putIntVolatile(writeIndex + BufferSlice.FREE_MARK_FIELD_OFFSET, underlying.capacity());
  }
}
