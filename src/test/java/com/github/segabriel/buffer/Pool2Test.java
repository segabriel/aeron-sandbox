package com.github.segabriel.buffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

class Pool2Test {

  static {
    System.setProperty("buffer.slice.debug", "true");
  }

  private Random random = new Random();

  @Test
  void test() {

    BufferPool pool = new BufferPool(100);

    Flux.range(0, 1000000)
        .delayElements(Duration.ofMillis(50), Schedulers.single())
        .flatMap(
            i -> {
              int size = 3 + random.nextInt(10);
              byte[] msg = randomArray(size);
              BufferSlice slice = pool.allocate(msg.length).putBytes(0, msg);

              return Mono.delay(Duration.ofMillis(25), Schedulers.parallel())
                  .doOnSuccess(
                      $ -> {
                        byte[] actual = new byte[msg.length];
                        slice.getBytes(0, actual);
                        assertArrayEquals(msg, actual);
                        slice.release();
                      });
            }, Integer.MAX_VALUE)
        .doOnError(Throwable::printStackTrace)
        .blockLast();
  }

  private byte[] randomArray(int size) {
    return RandomStringUtils.randomAlphabetic(size).getBytes(StandardCharsets.UTF_8);
  }
}
