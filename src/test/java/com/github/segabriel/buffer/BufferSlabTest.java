package com.github.segabriel.buffer;

import static com.github.segabriel.buffer.BufferSlice.HEADER_OFFSET;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class BufferSlabTest {

  static {
    System.setProperty("buffer.slice.debug", "true");
  }

  private static final Random RANDOM = new Random(42);
  private static final int MAX_MESSAGE_LENGTH = 200;
  private static final int BUFFER_LENGTH = MAX_MESSAGE_LENGTH + HEADER_OFFSET;

  private UnsafeBuffer underlying;
  private BufferSlab slab;

  @BeforeEach
  void setUp() {
    underlying = new UnsafeBuffer(ByteBuffer.allocate(BUFFER_LENGTH));
    slab = new BufferSlab(underlying);
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \---\
   *   |r--w--------|
   * </pre>
   */
  @Test
  void testFirstCallSizeLessThanMaxMsgSize() {
    byte[] msgBytes = randomArray(12);

    BufferSlice slice = slab.allocate(msgBytes.length).putBytes(0, msgBytes);

    assertArrayEquals(msgBytes, getUnderlyingBytes(slice.offset(), slice.capacity()));
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \------------\
   *   |r--w--------|
   * </pre>
   */
  @Test
  void testFirstCallSizeEqualsMaxMsgSize() {
    byte[] msgBytes = randomArray(MAX_MESSAGE_LENGTH);

    BufferSlice slice = slab.allocate(MAX_MESSAGE_LENGTH);
    slice.putBytes(0, msgBytes);

    assertArrayEquals(msgBytes, getUnderlyingBytes(slice.offset(), slice.capacity()));
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \-------------\
   *   |r--w--------|
   * </pre>
   */
  @Test
  void testFirstCallSizeMoreThanMaxMsgSize() {
    assertNull(slab.allocate(MAX_MESSAGE_LENGTH + 1));
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \----\  \----\
   *   |r----------w|
   * </pre>
   */
  @Test
  void testFullFill() {
    byte[] msg1Bytes = randomArray(MAX_MESSAGE_LENGTH / 2);
    byte[] msg2Bytes = randomArray(BUFFER_LENGTH - 2 * HEADER_OFFSET - msg1Bytes.length);

    BufferSlice slice1 = slab.allocate(msg1Bytes.length).putBytes(0, msg1Bytes);
    BufferSlice slice2 = slab.allocate(msg2Bytes.length).putBytes(0, msg2Bytes);

    assertArrayEquals(msg1Bytes, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \----\  \----\ \----\
   *   |r----------w|
   * </pre>
   */
  @Test
  void testFullFillNoSpace() {
    byte[] msg1Bytes = randomArray(MAX_MESSAGE_LENGTH / 2);
    byte[] msg2Bytes = randomArray(BUFFER_LENGTH - 2 * HEADER_OFFSET - msg1Bytes.length);
    byte[] msg3Bytes = randomArray(msg2Bytes.length);

    BufferSlice slice1 = slab.allocate(msg1Bytes.length).putBytes(0, msg1Bytes);
    BufferSlice slice2 = slab.allocate(msg2Bytes.length).putBytes(0, msg2Bytes);

    assertNull(slab.allocate(msg3Bytes.length));
    assertArrayEquals(msg1Bytes, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \--\  \--\ \---\
   *   |r-------w---|
   * </pre>
   */
  @Test
  void testAllocateNoSpace() {
    byte[] msg1Bytes = randomArray(MAX_MESSAGE_LENGTH / 3);
    byte[] msg2Bytes = randomArray(MAX_MESSAGE_LENGTH / 3);
    byte[] msg3Bytes = randomArray(MAX_MESSAGE_LENGTH / 3);

    BufferSlice slice1 = slab.allocate(msg1Bytes.length).putBytes(0, msg1Bytes);
    BufferSlice slice2 = slab.allocate(msg2Bytes.length).putBytes(0, msg2Bytes);

    assertNull(slab.allocate(msg3Bytes.length));
    assertArrayEquals(msg1Bytes, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \--\  \--\
   *   |r-------w---|
   * </pre>
   */
  @Test
  void testPartiallyFilled() {
    byte[] msg1Bytes = randomArray(MAX_MESSAGE_LENGTH / 3);
    byte[] msg2Bytes = randomArray(MAX_MESSAGE_LENGTH / 3);

    BufferSlice slice1 = slab.allocate(msg1Bytes.length).putBytes(0, msg1Bytes);
    BufferSlice slice2 = slab.allocate(msg2Bytes.length).putBytes(0, msg2Bytes);

    assertArrayEquals(msg1Bytes, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \--\ \--\
   *   |r------w----|
   * $ \--\
   *   |----r--w----|
   * & \--\
   *   |----r------w|
   * </pre>
   */
  @Test
  void test2() {
    int size = BUFFER_LENGTH - 3 * HEADER_OFFSET;
    byte[] msg1Bytes = randomArray(size / 3);
    byte[] msg2Bytes = randomArray(size / 3);
    byte[] msg3Bytes = randomArray(size / 3);

    BufferSlice slice1 = slab.allocate(msg1Bytes.length).putBytes(0, msg1Bytes);
    BufferSlice slice2 = slab.allocate(msg2Bytes.length).putBytes(0, msg2Bytes);

    assertArrayEquals(msg1Bytes, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));

    slice1.release();

    BufferSlice slice3 = slab.allocate(msg3Bytes.length).putBytes(0, msg3Bytes);

    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));
    assertArrayEquals(msg3Bytes, getUnderlyingBytes(slice3.offset(), slice3.capacity()));
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \--\ \--\
   *   |r------w----|
   * $ \--\
   *   |----r--w----|
   * & \--\
   *   |----r------w|
   * & \--\
   *   |----b-------|
   * </pre>
   */
  @Test
  void test3() {
    int size = BUFFER_LENGTH - 3 * HEADER_OFFSET;
    byte[] msg1Bytes = randomArray(size / 3);
    byte[] msg2Bytes = randomArray(size / 3);
    byte[] msg3Bytes = randomArray(size / 3);
    byte[] msg4Bytes = randomArray(size / 3);

    BufferSlice slice1 = slab.allocate(msg1Bytes.length).putBytes(0, msg1Bytes);
    BufferSlice slice2 = slab.allocate(msg2Bytes.length).putBytes(0, msg2Bytes);

    assertArrayEquals(msg1Bytes, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));

    slice1.release();

    BufferSlice slice3 = slab.allocate(msg3Bytes.length).putBytes(0, msg3Bytes);
    BufferSlice slice4 = slab.allocate(msg4Bytes.length).putBytes(0, msg4Bytes);

    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));
    assertArrayEquals(msg3Bytes, getUnderlyingBytes(slice3.offset(), slice3.capacity()));
    assertArrayEquals(msg4Bytes, getUnderlyingBytes(slice4.offset(), slice4.capacity()));
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \--\ \--\
   *   |r------w----|
   * $ \--\
   *   |----r--w----|
   * & \--\
   *   |----r------w|
   * & \--\
   *   |----b-------|
   * & \--\
   * </pre>
   */
  @Test
  void test4() {
    int size = BUFFER_LENGTH - 3 * HEADER_OFFSET;
    byte[] msg1Bytes = randomArray(size / 3);
    byte[] msg2Bytes = randomArray(size / 3);
    byte[] msg3Bytes = randomArray(size / 3);
    byte[] msg4Bytes = randomArray(size / 3);
    byte[] msg5Bytes = randomArray(size / 3);

    BufferSlice slice1 = slab.allocate(msg1Bytes.length).putBytes(0, msg1Bytes);
    BufferSlice slice2 = slab.allocate(msg2Bytes.length).putBytes(0, msg2Bytes);

    assertArrayEquals(msg1Bytes, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));

    slice1.release();

    BufferSlice slice3 = slab.allocate(msg3Bytes.length).putBytes(0, msg3Bytes);
    BufferSlice slice4 = slab.allocate(msg4Bytes.length).putBytes(0, msg4Bytes);

    assertNull(slab.allocate(msg5Bytes.length));
    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));
    assertArrayEquals(msg3Bytes, getUnderlyingBytes(slice3.offset(), slice3.capacity()));
    assertArrayEquals(msg4Bytes, getUnderlyingBytes(slice4.offset(), slice4.capacity()));
  }

  /**
   *
   *
   * <pre>
   *   |b-----------|
   * & \--\ \--\
   *   |r------w----|
   * $ \--\
   *   |----r--w----|
   * & \--\
   *   |----r------w|
   * & \--\
   *   |----b-------|
   * $ \--\ \--\
   *   |----b-------|
   * & \-\ \-\
   *   |---------wr-|
   * </pre>
   */
  @Test
  void test5() {
    underlying = new UnsafeBuffer(ByteBuffer.allocate(100));
    slab = new BufferSlab(underlying);

    byte[] msg1Bytes = randomArray(33 - 5);
    byte[] msg2Bytes = randomArray(33 - 5);
    byte[] msg3Bytes = randomArray(33 - 5);
    byte[] msg4Bytes = randomArray(33 - 5);

    byte[] msg5Bytes = randomArray(33 - 5);

    byte[] msg6Bytes = randomArray(20 - 5);
    byte[] msg7Bytes = randomArray(20 - 5);
    byte[] msg8Bytes = randomArray(20 - 5);
    byte[] msg9Bytes = randomArray(20 - 5);

    BufferSlice slice1 = slab.allocate(msg1Bytes.length).putBytes(0, msg1Bytes);
    BufferSlice slice2 = slab.allocate(msg2Bytes.length).putBytes(0, msg2Bytes);

    assertArrayEquals(msg1Bytes, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));

    slice1.release();

    BufferSlice slice3 = slab.allocate(msg3Bytes.length).putBytes(0, msg3Bytes);
    BufferSlice slice4 = slab.allocate(msg4Bytes.length).putBytes(0, msg4Bytes);

    assertNull(slab.allocate(msg5Bytes.length));
    assertArrayEquals(msg2Bytes, getUnderlyingBytes(slice2.offset(), slice2.capacity()));
    assertArrayEquals(msg3Bytes, getUnderlyingBytes(slice3.offset(), slice3.capacity()));
    assertArrayEquals(msg4Bytes, getUnderlyingBytes(slice4.offset(), slice4.capacity()));

    slice2.release();
    slice3.release();

    BufferSlice slice6 = slab.allocate(msg6Bytes.length).putBytes(0, msg6Bytes);
    BufferSlice slice7 = slab.allocate(msg7Bytes.length).putBytes(0, msg7Bytes);
    BufferSlice slice8 = slab.allocate(msg8Bytes.length).putBytes(0, msg8Bytes);

    assertArrayEquals(msg4Bytes, getUnderlyingBytes(slice4.offset(), slice4.capacity()));
    assertArrayEquals(msg6Bytes, getUnderlyingBytes(slice6.offset(), slice6.capacity()));
    assertArrayEquals(msg7Bytes, getUnderlyingBytes(slice7.offset(), slice7.capacity()));
    assertArrayEquals(msg8Bytes, getUnderlyingBytes(slice8.offset(), slice8.capacity()));

    slice4.release();

    BufferSlice slice9 = slab.allocate(msg9Bytes.length).putBytes(0, msg9Bytes);

    assertArrayEquals(msg6Bytes, getUnderlyingBytes(slice6.offset(), slice6.capacity()));
    assertArrayEquals(msg7Bytes, getUnderlyingBytes(slice7.offset(), slice7.capacity()));
    assertArrayEquals(msg8Bytes, getUnderlyingBytes(slice8.offset(), slice8.capacity()));
    assertArrayEquals(msg9Bytes, getUnderlyingBytes(slice9.offset(), slice9.capacity()));

    byte[] msg10Bytes = randomArray(13 - 5);

    BufferSlice slice10 = slab.allocate(msg10Bytes.length).putBytes(0, msg10Bytes);
  }

  @Test
  void test6() {
    underlying = new UnsafeBuffer(ByteBuffer.allocate(5 + 1024 + 5 + 1024));
    slab = new BufferSlab(underlying);

    byte[] msg1 = randomArray(1024);
    byte[] msg2 = randomArray(1024);
    byte[] msg3 = randomArray(1024);

    BufferSlice slice1 = slab.allocate(msg1.length).putBytes(0, msg1);
    BufferSlice slice2 = slab.allocate(msg2.length).putBytes(0, msg2);

    assertArrayEquals(msg1, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
    assertArrayEquals(msg2, getUnderlyingBytes(slice2.offset(), slice2.capacity()));

    slice1.release();

    BufferSlice slice3 = slab.allocate(msg3.length).putBytes(0, msg3);

    assertArrayEquals(msg2, getUnderlyingBytes(slice2.offset(), slice2.capacity()));
    assertArrayEquals(msg3, getUnderlyingBytes(slice3.offset(), slice3.capacity()));
  }

  @Test
  @Disabled // it doesn't support behavior - slice1 blocks all, we move only from left to right.
  void test7() {
    int msgSize = 40;
    underlying =
        new UnsafeBuffer(ByteBuffer.allocate(HEADER_OFFSET + msgSize + HEADER_OFFSET + msgSize));
    slab = new BufferSlab(underlying);

    byte[] msg1 = randomArray(msgSize);
    BufferSlice slice1 = slab.allocate(msg1.length).putBytes(0, msg1);

    assertArrayEquals(msg1, getUnderlyingBytes(slice1.offset(), slice1.capacity()));

    for (int i = 0; i < 10; i++) {
      byte[] msg2 = randomArray(msgSize);
      BufferSlice slice2 = slab.allocate(msg2.length).putBytes(0, msg2);

      assertArrayEquals(msg1, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
      assertArrayEquals(msg2, getUnderlyingBytes(slice2.offset(), slice2.capacity()));

      slice2.release();
    }
  }

  @Test
  void test8() {
    int msgSize = 40;
    underlying =
        new UnsafeBuffer(ByteBuffer.allocate(HEADER_OFFSET + msgSize + HEADER_OFFSET + msgSize));
    slab = new BufferSlab(underlying);

    byte[] msg1 = randomArray(msgSize);
    byte[] msg2 = randomArray(msgSize);
    BufferSlice slice1 = slab.allocate(msg1.length).putBytes(0, msg1);
    BufferSlice slice2 = slab.allocate(msg2.length).putBytes(0, msg2);

    assertArrayEquals(msg1, getUnderlyingBytes(slice1.offset(), slice1.capacity()));
    assertArrayEquals(msg2, getUnderlyingBytes(slice2.offset(), slice2.capacity()));

    slice1.release();

    for (int i = 0; i < 10; i++) {
      byte[] msg3 = randomArray(msgSize);
      BufferSlice slice3 = slab.allocate(msg3.length).putBytes(0, msg3);

      assertArrayEquals(msg2, getUnderlyingBytes(slice2.offset(), slice2.capacity()));
      assertArrayEquals(msg3, getUnderlyingBytes(slice3.offset(), slice3.capacity()));

      slice3.release();
    }
  }

  private byte[] getUnderlyingBytes(int offset, int length) {
    byte[] bytes = new byte[length];
    underlying.getBytes(offset, bytes);
    return bytes;
  }

  private byte[] randomArray(int size) {
    return RandomStringUtils.randomAlphabetic(size).getBytes(StandardCharsets.UTF_8);
  }
}
