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
import org.preta.tools.ozone.benchmark.IoStats;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Command(name = "write",
    description = "Benchmark OzoneManager Write.",
    mixinStandardHelpOptions = true)
public class OmWriteBenchmark extends AbstractOmBenchmark
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

  private final AtomicLong writeKeyNamePointer;

  public OmWriteBenchmark() {
    this.user = "admin";
    this.volume = "instagram";
    this.bucket = "images";
    this.keyNamePrefix = "";
    this.writerThreads = 10;
    this.writeKeyNamePointer = new AtomicLong(0);
  }

  public void execute() {
    try {
      keyNamePrefix += UUID.randomUUID().toString();
      System.out.println("Benchmarking OzoneManager Write.");
      final long endTimeInNs = getIoStats().getStartTime() + (runtime * 1000000000L);
      createVolume(user, volume);
      createBucket(volume, bucket);

      ExecutorService writeExecutor = Executors.newFixedThreadPool(writerThreads);
      for (int i = 0; i < this.writerThreads; i++) {
        writeExecutor.submit(() -> {
          while (System.nanoTime() < endTimeInNs) {
            writeKey(volume, bucket, getKeyNameToWrite());
          }
        });
      }

      ScheduledExecutorService statsThread = Executors.newSingleThreadScheduledExecutor();
      statsThread.scheduleAtFixedRate(this::printStats, 5, 5, TimeUnit.MINUTES);

      writeExecutor.shutdown();
      writeExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
      statsThread.shutdown();
      statsThread.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
    } catch (Exception ex) {
      System.err.println("Encountered Exception:");
      ex.printStackTrace();
    }
  }

  private String getKeyNameToWrite() {
    return keyNamePrefix + "-" + writeKeyNamePointer.incrementAndGet();
  }

  public void printStats() {
    final DecimalFormat df = new DecimalFormat("#.0000");
    final IoStats stats = getIoStats();
    System.out.println("================================================================");
    System.out.println(LocalDateTime.now());
    System.out.println("================================================================");
    System.out.println("Time elapsed: " + (stats.getElapsedTime() / 1000000000) + " sec.");
    System.out.println("Number of Threads: " + writerThreads);
    System.out.println("Number of Keys written: " + stats.getKeysCreated());
    System.out.println("Average Key write time (CPU Time): " + df.format(stats.getAverageKeyWriteCpuTime() / 1000000) + " milliseconds. ");
    System.out.println("Average Key write time (Real): " + df.format((stats.getAverageKeyWriteCpuTime() / writerThreads) / 1000000 ) + " milliseconds. ");
    System.out.println("Max Key write time (CPU Time): " + stats.getMaxKeyWriteTime() / 1000000 + " milliseconds.");
    System.out.println("****************************************************************");

  }

}
