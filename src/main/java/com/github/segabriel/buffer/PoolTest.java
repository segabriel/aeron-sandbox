package com.github.segabriel.buffer;

import java.nio.charset.StandardCharsets;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Random;

public class PoolTest {

  public static void main(String[] args) throws InterruptedException {
    String msg = "hello";

    BufferPool pool = new BufferPool(10240);

    Random random = new Random();

    for (int i = 0; i < 1000000; i++) {


      Thread.sleep(50);
      BufferSlice slice = pool.allocate(23 + random.nextInt(500));
      slice.putBytes(0, msg.getBytes(StandardCharsets.UTF_8));
//      slice.print();
      Mono.delay(Duration.ofMillis(50)).doOnSuccess($ -> slice.release()).subscribe();
    }
  }
}
