/*
 * Copyright 2019 Nandakumar
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

package org.preta.tools.ozone.benchmark.om;

import org.preta.tools.ozone.ReadableTimestampConverter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Command(name = "read-write",
    description = "Benchmark OzoneManager Read/Write.",
    mixinStandardHelpOptions = true)
public class OmReadWriteBenchmark extends AbstractOmBenchmark
{

  @Option(names = {"-d", "--duration"},
      required = true,
      converter = ReadableTimestampConverter.class,
      description = "Runtime. Can be specified in seconds, minutes or hours " +
          "using the s, m or h suffixes respectively. Default unit is seconds.")
  private long runtime;

  @Option(names = {"-u", "--user"},
      description = "User Name.")
  private String user;

  @Option(names = {"-v", "--volume"},
      description = "Ozone Volume.")
  private String volume;

  @Option(names = {"-b", "--bucket"},
      description = "Ozone Bucket.")
  private String bucket;

  @Option(names = {"-p", "--keyPrefix"},
      description = "Key Prefix.")
  private String keyNamePrefix;

  @Option(names = {"-w", "--numWriteThreads"},
      description = "Number of writer threads.")
  private int writerThreads;

  @Option(names = {"-r", "--numReaderThreads"},
      description = "Number of reader threads.")
  private int readerThreads;

  private final AtomicLong writeKeyNamePointer;
  private final AtomicLong readKeyNamePointer;

  public OmReadWriteBenchmark() {
    this.user = "admin";
    this.volume = "instagram";
    this.bucket = "images";
    this.keyNamePrefix = "";
    this.writerThreads = 10;
    this.readerThreads = 10;
    this.writeKeyNamePointer = new AtomicLong(0);
    this.readKeyNamePointer = new AtomicLong(0);
  }

  public void execute() {
    try {
      keyNamePrefix += UUID.randomUUID().toString();
      System.out.println("Benchmarking OzoneManager Read/Write.");
      final long endTimeInNs = System.nanoTime() + (runtime * 1000000000L);
      createVolume(user, volume);
      createBucket(volume, bucket);

      // Warming up!
      for (int i = 0; i < 1000; i++) {
        writeKey(volume, bucket, getKeyNameToWrite());
      }

      ExecutorService writeExecutor = Executors.newFixedThreadPool(writerThreads);
      ExecutorService readExecutor = Executors.newFixedThreadPool(readerThreads);

      for (int i = 0; i < this.writerThreads; i++) {
        writeExecutor.submit(() -> {
          while (System.nanoTime() < endTimeInNs) {
            writeKey(volume, bucket, getKeyNameToWrite());
          }
        });
      }

      for (int i = 0; i < this.readerThreads; i++) {
        readExecutor.submit(() -> {
          while (System.nanoTime() < endTimeInNs) {
            readKey(volume, bucket, getKeyNameToRead());
          }
        });
      }

      writeExecutor.shutdown();
      readExecutor.shutdown();
      writeExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
      readExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
    } catch (Exception ex) {
      System.err.println("Encountered Exception:");
      ex.printStackTrace();
    }
  }

  private String getKeyNameToWrite() {
    return keyNamePrefix + "-" + writeKeyNamePointer.incrementAndGet();
  }

  private String getKeyNameToRead() {
    if (readKeyNamePointer.get() > getIoStats().getKeysCreated()) {
      readKeyNamePointer.set(0L);
    }
    return keyNamePrefix + "-" + readKeyNamePointer.incrementAndGet();
  }

  public void printStats() {

  }
}
