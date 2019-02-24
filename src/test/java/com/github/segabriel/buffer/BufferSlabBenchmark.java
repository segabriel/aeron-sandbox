package com.github.segabriel.buffer;

import static com.github.segabriel.buffer.BufferSlice.HEADER_OFFSET;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
@Fork(value = 2)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
public class BufferSlabBenchmark {

  private static final int MSG_LENGTH = 1024;
  private static final int BUFFER_LENGTH = HEADER_OFFSET + MSG_LENGTH + HEADER_OFFSET + MSG_LENGTH;

  private final UnsafeBuffer underlying =
      new UnsafeBuffer(ByteBuffer.allocateDirect(BUFFER_LENGTH));
  private final BufferSlab slab = new BufferSlab(underlying);

  @Setup
  public void setUp() {
    BufferSlice slice1 = slab.allocate(MSG_LENGTH);
    BufferSlice slice2 = slab.allocate(MSG_LENGTH);
    slice1.release();
  }

  @Benchmark
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  public void allocate(Blackhole blackhole) {
    BufferSlice bufferSlice = slab.allocate(MSG_LENGTH);
    if (bufferSlice == null) {
      throw new NullPointerException();
    }
    blackhole.consume(bufferSlice);
    bufferSlice.release();
  }

  public static void main(String[] args) throws Exception {
    org.openjdk.jmh.Main.main(args);
  }
}
