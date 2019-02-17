package com.github.segabriel.buffer;

import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.concurrent.UnsafeBuffer;

public class BufferSlab {

  private static final AtomicInteger COUNTER = new AtomicInteger();

  public final int id = COUNTER.incrementAndGet();
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
        try {
//        return slice(wIndex, fullLength, nextReadOffset(rIndex));
        return slice(wIndex, fullLength);
        } catch (Exception e) {
          throw e;
        }
      }

      if (!isReleased(rIndex)) {
        return null;
      }

      while (isReleased(rIndex)) {
        int nextOffset = nextReadOffset(rIndex);

        if (nextOffset == rIndex) { //todo
          this.readIndex = rIndex;
          System.err.println("dsa");
          return null;
        }

        if (nextOffset == wIndex || nextOffset == this.writeIndex) {
          // whole buffer is available, reset all index
          wIndex = 0;
          reset();
          availableBytes = underlying.capacity();
          if (availableBytes >= fullLength) {
            this.writeIndex = wIndex + fullLength;
//            return slice(wIndex, fullLength, nextOffset);
            return slice(wIndex, fullLength);
          }
          return null;
        }
        rIndex = nextOffset;

        if (rIndex == 0 && wIndex == 0) {
          System.out.println();
        }

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
//          return slice(wIndex, fullLength, nextOffset);
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
//        return slice(wIndex, fullLength, nextReadOffset(rIndex));
        return slice(wIndex, fullLength);
      }

      wIndex = 0;
      return allocate(fullLength, wIndex, rIndex);
    }

    if (isReleased(rIndex)) { // rIndex == wIndex
      int nextOffset = nextReadOffset(rIndex);
      if (nextOffset == rIndex) {
        // whole buffer is available
        if (rIndex != 0) {
          wIndex = 0;
          rIndex = 0;
          reset();
        }
        int availableBytes = underlying.capacity();
        this.readIndex = rIndex;
        if (availableBytes >= fullLength) {
          this.writeIndex = wIndex + fullLength;
//          return slice(wIndex, fullLength, nextOffset);
          return slice(wIndex, fullLength);
        }
        return null;
      }

//      int availableBytes = nextOffset - rIndex;
//      if (availableBytes >= fullLength) {
//        this.writeIndex = wIndex + fullLength;
//        return slice(wIndex, fullLength);
//      }

//      if (!isReleased(nextOffset)) {
//        return null;
//      }

//      if (nextOffset < 1) {
//        return null;
//      }
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
   * @param currentReadOffset current offset
   * @return next offset
   */
  private int nextReadOffset(int currentReadOffset) {
    int i = underlying.getInt(currentReadOffset + BufferSlice.FREE_MARK_FIELD_OFFSET);
    if (i >= underlying.capacity()) {
      return 0; // todo workaround
    }
    return i;
  }

  private boolean isReleased(int offset) {
    return underlying.getByteVolatile(offset) == 0;
  }

  private BufferSlice slice(int offset, int fullLength) {
    underlying.putByte(offset, (byte) '?');
    int nextReadOffset = offset + fullLength;
    if (nextReadOffset + BufferSlice.HEADER_OFFSET >= underlying.capacity()) {
      nextReadOffset = 0;
    }
//    else {
//      underlying.putByte(nextReadOffset, (byte) 0);
//      underlying.putInt(nextReadOffset + BufferSlice.FREE_MARK_FIELD_OFFSET, 0);
//    }
    underlying.putInt(offset + BufferSlice.FREE_MARK_FIELD_OFFSET, nextReadOffset);
    return new BufferSlice(id, underlying, offset, fullLength);
  }

  private BufferSlice slice3(int offset, int fullLength) {
    underlying.putByte(offset, (byte) '?');
    int nextReadOffset = offset + fullLength;
    if (nextReadOffset + BufferSlice.HEADER_OFFSET >= underlying.capacity()) {
      underlying.putInt(offset + BufferSlice.FREE_MARK_FIELD_OFFSET, 0);
    } else {

//      nextReadOffset(offset)

      underlying.putInt(offset + BufferSlice.FREE_MARK_FIELD_OFFSET, nextReadOffset);
    }


    return new BufferSlice(id, underlying, offset, fullLength);
  }

  private BufferSlice slice(int offset, int fullLength, int oldNextReadOffset) {
    underlying.putByte(offset, (byte) '?');
    int nextReadOffset = offset + fullLength;

    if (oldNextReadOffset >= nextReadOffset) {
      if (oldNextReadOffset - nextReadOffset >= BufferSlice.HEADER_OFFSET) {
        underlying.putByte(nextReadOffset, (byte) 0);
        underlying.putInt(nextReadOffset + BufferSlice.FREE_MARK_FIELD_OFFSET, oldNextReadOffset);
        if (oldNextReadOffset< 0 || oldNextReadOffset >= underlying.capacity()) {
          System.out.println(oldNextReadOffset);
        }
      } else {
        nextReadOffset = oldNextReadOffset;
      }
    } else { // offset == oldNextReadOffset == 0
      if (nextReadOffset + BufferSlice.HEADER_OFFSET >= underlying.capacity()) {
        nextReadOffset = 0;
      } else {
        underlying.putByte(nextReadOffset, (byte) 0);
        underlying.putInt(nextReadOffset + BufferSlice.FREE_MARK_FIELD_OFFSET, underlying.capacity() - nextReadOffset);

        if (underlying.capacity() - nextReadOffset< 0 || underlying.capacity() - nextReadOffset >= underlying.capacity()) {
          System.out.println(underlying.capacity() - nextReadOffset);
        }
      }
    }

//
//
//    if (nextReadOffset + BufferSlice.HEADER_OFFSET >= underlying.capacity()) {
//      nextReadOffset = 0;
//    } else {
//      underlying.putByte(nextReadOffset, (byte) 0);
//      if ((oldNextReadOffset - offset) > BufferSlice.HEADER_OFFSET) {
//        underlying.putInt(nextReadOffset + BufferSlice.FREE_MARK_FIELD_OFFSET, oldNextReadOffset);
//      }
//
//
//      underlying.putInt(nextReadOffset + BufferSlice.FREE_MARK_FIELD_OFFSET, 0);
//    }
    underlying.putInt(offset + BufferSlice.FREE_MARK_FIELD_OFFSET, nextReadOffset);
    if (nextReadOffset< 0 || nextReadOffset >= underlying.capacity()) {
      System.out.println(nextReadOffset);
    }

    return new BufferSlice(id, underlying, offset, fullLength);
  }

  private void reset() {
    this.writeIndex = 0;
    this.readIndex = 0;
    underlying.putByte(writeIndex, (byte) 0);
    underlying.putInt(writeIndex + BufferSlice.FREE_MARK_FIELD_OFFSET, 0);
  }
}
