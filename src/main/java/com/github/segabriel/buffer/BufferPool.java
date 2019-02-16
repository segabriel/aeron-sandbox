package com.github.segabriel.buffer;

import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class BufferPool {

  private final int slabCapacity;
  private final List<BufferSlab> slabs;

  public BufferPool(int slabCapacity) {
    this.slabCapacity = slabCapacity;
    BufferSlab slab = new BufferSlab(new UnsafeBuffer(ByteBuffer.allocateDirect(slabCapacity)));
    this.slabs = new CopyOnWriteArrayList<>();
    slabs.add(slab);
  }

  public BufferSlice allocate(int size) {
    for (BufferSlab slab : slabs) {
      BufferSlice slice = slab.allocate(size);
      if (slice != null) {
        return slice;
      }
    }
    BufferSlab slab = new BufferSlab(new UnsafeBuffer(ByteBuffer.allocateDirect(slabCapacity)));
    slabs.add(slab);

    System.err.println(" created new slab,  " + slabs.size());
    return slab.allocate(size);
  }
}
