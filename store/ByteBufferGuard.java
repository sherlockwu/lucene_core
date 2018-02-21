/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.Constants;

/**
 * A guard that is created for every {@link ByteBufferIndexInput} that tries on best effort
 * to reject any access to the {@link ByteBuffer} behind, once it is unmapped. A single instance
 * of this is used for the original and all clones, so once the original is closed and unmapped
 * all clones also throw {@link AlreadyClosedException}, triggered by a {@link NullPointerException}.
 * <p>
 * This code tries to hopefully flush any CPU caches using a store-store barrier. It also yields the
 * current thread to give other threads a chance to finish in-flight requests...
 */
final class ByteBufferGuard {

  /**
   * Pass in an implementation of this interface to cleanup ByteBuffers.
   * MMapDirectory implements this to allow unmapping of bytebuffers with private Java APIs.
   */
  @FunctionalInterface
  static interface BufferCleaner {
    void freeBuffer(String resourceDescription, ByteBuffer b) throws IOException;
  }

  private final String resourceDescription;
  private final BufferCleaner cleaner;

  /** Not volatile; see comments on visibility below! */
  private boolean invalidated = false;

  /** Used as a store-store barrier; see comments below! */
  private final AtomicInteger barrier = new AtomicInteger();

  /**
   * Creates an instance to be used for a single {@link ByteBufferIndexInput} which
   * must be shared by all of its clones.
   */
  public ByteBufferGuard(String resourceDescription, BufferCleaner cleaner) {
    this.resourceDescription = resourceDescription;
    this.cleaner = cleaner;
  }
  /*Kan For Print
  */

  private long threshold = 50000;
  private String Log = "";

  /**
   * Invalidates this guard and unmaps (if supported).
   */
  public void invalidateAndUnmap(ByteBuffer... bufs) throws IOException {
    System.out.printf("==== Get to the end of Guard with %d size buffer \n", Log.length());
    if (cleaner != null) {
      invalidated = true;
      // This call should hopefully flush any CPU caches and as a result make
      // the "invalidated" field update visible to other threads. We specifically
      // don't make "invalidated" field volatile for performance reasons, hoping the
      // JVM won't optimize away reads of that field and hardware should ensure
      // caches are in sync after this call. This isn't entirely "fool-proof"
      // (see LUCENE-7409 discussion), but it has been shown to work in practice
      // and we count on this behavior.
      barrier.lazySet(0);
      // we give other threads a bit of time to finish reads on their ByteBuffer...:
      Thread.yield();
      // finally unmap the ByteBuffers:
      for (ByteBuffer b : bufs) {
        cleaner.freeBuffer(resourceDescription, b);
      }
    }
  }

  private void ensureValid() {
    if (invalidated) {
      // this triggers an AlreadyClosedException in ByteBufferIndexInput:
      throw new NullPointerException();
    }
  }

  public void getBytes(ByteBuffer receiver, byte[] dst, int offset, int length) {
    long startTime;
    if (Constants.DEBUG) {
      startTime = System.nanoTime();
    }
    ensureValid();
    receiver.get(dst, offset, length);
    if (Constants.DEBUG) {
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      if (duration > threshold)
        Log += "=== getBytes: " +  duration + "\n";
        System.out.printf("=== getBytes: %d \n", duration);
    }
  }

  public byte getByte(ByteBuffer receiver) {
    long startTime;
    if (Constants.DEBUG) {
      startTime = System.nanoTime();
    }
    ensureValid();
    //return receiver.get();
    try {
      return receiver.get();
    } finally {
    if (Constants.DEBUG) {
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      if (duration > threshold)
        Log += "=== getByte: " +  duration + "\n";
        System.out.printf("=== getByte: %d \n", duration);
    }
    }
  }

  public short getShort(ByteBuffer receiver) {
    long startTime;
    if (Constants.DEBUG) {
      startTime = System.nanoTime();
    }
    ensureValid();
    try {
      return receiver.getShort();
    } finally {
    if (Constants.DEBUG) {
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      if (duration > threshold)
        Log += "=== getShort: " +  duration + "\n";
        System.out.printf("=== getShort: %d \n", duration);
    }
    }
  }

  public int getInt(ByteBuffer receiver) {
    long startTime;
    if (Constants.DEBUG) {
      startTime = System.nanoTime();
    }
    ensureValid();
    try {
      return receiver.getInt();
    } finally {
    if (Constants.DEBUG) {
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      if (duration > threshold)
        Log += "=== getInt: " +  duration + "\n";
        System.out.printf("=== getInt: %d \n", duration);
    }
    }
  }

  public long getLong(ByteBuffer receiver) {
    long startTime;
    if (Constants.DEBUG) {
      startTime = System.nanoTime();
    }
    ensureValid();
    try {
      return receiver.getLong();
    } finally {
    if (Constants.DEBUG) {
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      if (duration > threshold)
        Log += "=== getLong: " +  duration + "\n";
        System.out.printf("=== getLong: %d \n", duration);
    }
    }
  }

  public byte getByte(ByteBuffer receiver, int pos) {
    long startTime;
    if (Constants.DEBUG) {
      startTime = System.nanoTime();
    }
    ensureValid();
    //return receiver.get(pos);
    try {
      return receiver.get(pos);
    } finally {
    if (Constants.DEBUG) {
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      if (duration > threshold)
        Log += "=== getByte(pos): " +  duration + "\n";
        System.out.printf("=== getByte(pos): %d \n", duration);
    }
      
    }
  }

  public short getShort(ByteBuffer receiver, int pos) {
    long startTime;
    if (Constants.DEBUG) {
      startTime = System.nanoTime();
    }
    ensureValid();
    try{
      return receiver.getShort(pos);
    } finally {
    if (Constants.DEBUG) {
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      if (duration > threshold)
        Log += "=== getShort(pos): " +  duration + "\n";
        System.out.printf("=== getShort(pos): %d \n", duration);
    }
    }
  }

  public int getInt(ByteBuffer receiver, int pos) {
    long startTime;
    if (Constants.DEBUG) {
      startTime = System.nanoTime();
    }
    ensureValid();
    try {
      return receiver.getInt(pos);
    } finally {
    if (Constants.DEBUG) {
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      if (duration > threshold)
        Log += "=== getInt(pos): " +  duration + "\n";
        System.out.printf("=== getInt(pos): %d \n", duration);
    }
    }
  }

  public long getLong(ByteBuffer receiver, int pos) {
    long startTime;
    if (Constants.DEBUG) {
      startTime = System.nanoTime();
    }
    ensureValid();
    try {
      return receiver.getLong(pos);
    } finally {
    if (Constants.DEBUG) {
      long endTime = System.nanoTime();
      long duration = (endTime - startTime);
      if (duration > threshold)
        Log += "=== getLong(pos): " +  duration + "\n";
        System.out.printf("=== getLong(pos): %d \n", duration);
    }
    }
  }

}
