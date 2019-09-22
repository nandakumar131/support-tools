/*
 * Copyright 2019 Nanda kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.preta.tools.ozone.benchmark;

import com.google.common.util.concurrent.AtomicDouble;

import java.util.concurrent.atomic.AtomicLong;

public class IoStats {

  // Write metrics.
  private final AtomicLong keysCreated = new AtomicLong(0);
  private final AtomicDouble totalCreateTimeNs = new AtomicDouble(0);
  private final AtomicDouble totalCloseTimeNs = new AtomicDouble(0);
  private final AtomicDouble writeElapsedTimeNs = new AtomicDouble(0);

  // Read metrics.
  private final AtomicLong keysRead = new AtomicLong(0);
  private final AtomicDouble totalOpenTimeNs = new AtomicDouble(0);
  private final AtomicDouble readElapsedTimeNs = new AtomicDouble(0);

  void incrKeysCreated() {
    keysCreated.incrementAndGet();
  }

  void addCreateTime(final long deltaNs) {
    totalCreateTimeNs.addAndGet(deltaNs);
  }

  void addCloseTime(final long deltaNs) {
    totalCloseTimeNs.addAndGet(deltaNs);
  }

  void addWriteElapsedTime(final long deltaNs) {
    writeElapsedTimeNs.addAndGet(deltaNs);
  }

  long getKeysCreated() {
    return keysCreated.get();
  }

  double getMeanCreateTimeMs() {
    return totalCreateTimeNs.get() / (keysCreated.get() * 1_000_000);
  }

  double getMeanCloseTimeMs() {
    return totalCloseTimeNs.get() / (keysCreated.get() * 1_000_000);
  }

  double getKeysWriteLatencyMs() {
    return writeElapsedTimeNs.get() / (keysCreated.get() * 1_000_000);
  }

  void incrKeysRead() {
    keysRead.incrementAndGet();
  }

  void addOpenTime(final long deltaNs) {
    totalOpenTimeNs.addAndGet(deltaNs);
  }

  void addReadElapsedTime(final long deltaNs) {
    readElapsedTimeNs.addAndGet(deltaNs);
  }

  double getMeanOpenTimeMs() {
    return totalOpenTimeNs.get() / (keysRead.get() * 1_000_000);
  }

  double getKeysReadLatencyMs() {
    return readElapsedTimeNs.get() / (keysRead.get() * 1_000_000);
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("Keys Created: ")
        .append(keysCreated.get())
        .append("\n")
        .append("Keys Read: ")
        .append(keysRead.get())
        .append("\n");
    return buffer.toString();
  }

}
