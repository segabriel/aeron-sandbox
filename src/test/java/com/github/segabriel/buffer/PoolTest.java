package com.github.segabriel.buffer;

import static com.github.segabriel.buffer.BufferSlice.HEADER_OFFSET;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.RepeatedTest;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

class PoolTest {

  static {
    System.setProperty("buffer.slice.debug", "false");
  }

  private Random random = new Random();

  @RepeatedTest(10)
  void test() {

    BufferPool pool = new BufferPool(100);

    Flux.range(0, Integer.MAX_VALUE)
        .delayElements(Duration.ofMillis(2), Schedulers.single())
        .flatMap(
            i -> {
              int size = 3 + random.nextInt(10);
              byte[] msg = randomArray(size);
              BufferSlice slice = pool.allocate(msg.length).putBytes(0, msg);

              return Mono.delay(Duration.ofMillis(10), Schedulers.parallel())
                  .doOnSuccess(
                      $ -> {
                        byte[] actual = new byte[msg.length];
                        slice.getBytes(0, actual);
                        assertArrayEquals(
                            msg,
                            actual,
                            "slice: offset="
                                + (slice.offset() - HEADER_OFFSET)
                                + ", nextOffset="
                                + (slice.offset() + slice.capacity()));
                        slice.release();
                      });
            },
            Integer.MAX_VALUE)
        .doOnError(Throwable::printStackTrace)
        .take(Duration.ofSeconds(10))
        .blockLast();
  }

  private byte[] randomArray(int size) {
    return RandomStringUtils.randomAlphabetic(size).getBytes(StandardCharsets.UTF_8);
  }
}
