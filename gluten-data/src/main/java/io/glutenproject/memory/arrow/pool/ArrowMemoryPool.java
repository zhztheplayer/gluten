package io.glutenproject.memory.arrow.pool;

import io.glutenproject.memory.alloc.NativeMemoryAllocator;

public class ArrowMemoryPool {
  private final long nativeInstanceId;

  public ArrowMemoryPool(long nativeInstanceId) {
    this.nativeInstanceId = nativeInstanceId;
  }

  public static ArrowMemoryPool getDefault() {
    return new ArrowMemoryPool(getDefaultArrowMemoryPool());
  }

  public static ArrowMemoryPool wrapWithAllocator(NativeMemoryAllocator alloc) {
    return new ArrowMemoryPool(createArrowMemoryPoolWithAllocator(alloc.getNativeInstanceId()));
  }

  public long getNativeInstanceId() {
    return this.nativeInstanceId;
  }

  public void close() {
    if (isDefaultMemoryPool()) {
      return;
    }
    releaseArrowMemoryPool(this.nativeInstanceId);
  }


  private boolean isDefaultMemoryPool() {
    return this.nativeInstanceId == getDefaultArrowMemoryPool();
  }

  private static native long getDefaultArrowMemoryPool();

  private static native long createArrowMemoryPoolWithAllocator(long allocId);

  private static native void releaseArrowMemoryPool(long memoryPoolId);
}
