package com.github.segabriel.buffer;

import org.agrona.concurrent.UnsafeBuffer;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class Test {

  public static void main(String[] args) throws InterruptedException {

    String msg = "hello";
    byte[] bytes = msg.getBytes(StandardCharsets.UTF_8);
//    int length = bytes.length;

    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);

    UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);

    BufferSlab allocator = new BufferSlab(unsafeBuffer);

    for (int i = 0; i < 1000; i++) {
      System.err.println(i);

      Thread.sleep(2);

      BufferSlice slice = allocator.allocate(100 - 5);
      if (slice != null) {
        slice.putBytes(0, ByteBuffer.wrap(bytes), 0, bytes.length);
        System.out.println(slice);
//        System.out.println("---------- received msg: " + slice.getStringUtf8(0, length));

        Mono.delay(Duration.ofMillis(100)).doOnSuccess($ -> slice.release()).subscribe();
      }
    }
  }
}
