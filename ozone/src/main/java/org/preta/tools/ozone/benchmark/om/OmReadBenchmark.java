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

import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;

import org.preta.tools.ozone.ReadableTimestampConverter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Command(name = "read",
    description = "Benchmark OzoneManager Read.",
    mixinStandardHelpOptions = true)
public class OmReadBenchmark  extends AbstractOmBenchmark {

  @Option(names = {"-d", "--duration"},
      required = true,
      converter = ReadableTimestampConverter.class,
      description = "Runtime. Can be specified in seconds, minutes or hours " +
          "using the s, m or h suffixes respectively. Default unit is seconds.")
  private long runtime;

  @Option(names = {"-v", "--volume"},
      description = "Ozone Volume.")
  private String volume;

  @Option(names = {"-b", "--bucket"},
      description = "Ozone Bucket.")
  private String bucket;

  @Option(names = {"-p", "--keyPrefix"},
      description = "Key Prefix.")
  private String keyNamePrefix;

  @Option(names = {"-r", "--numReaderThreads"},
      description = "Number of reader threads.")
  private int readerThreads;

  private List<String> keyNames;

  public OmReadBenchmark() {
    this.volume = "instagram";
    this.bucket = "images";
    this.keyNamePrefix = "";
    this.readerThreads = 10;
  }

  public void execute() {
    try {
      System.out.println("Benchmarking OzoneManager Read.");
      keyNames = getInputKeyNames();
      System.out.println("Found " + keyNames.size() + " keys with prefix '" + keyNamePrefix + "'.");
      try {
        final long endTimeInNs = System.nanoTime() + runtime * 1000000000L;
        final ExecutorService executor = Executors.newFixedThreadPool(readerThreads);
        for (int i = 0; i < readerThreads; i++) {
          executor.submit(() -> {
            while (System.nanoTime() < endTimeInNs) {
              readKey(volume, bucket, getKeyName());
            }
          });
        }
        executor.shutdown();
        executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
      } finally {
        System.out.println("Stopping...");
      }
    } catch (Exception ex) {
      System.err.println("Encountered Exception:");
      ex.printStackTrace();
    }
  }

  private List<String> getInputKeyNames() throws IOException {
    final List<String> keys = new ArrayList<>();
    final OzoneClient client = OzoneClientFactory.getRpcClient(getConfig());
    final Iterator<? extends OzoneKey> iter = client.getObjectStore()
        .getVolume(volume)
        .getBucket(bucket)
        .listKeys(keyNamePrefix);

    while (iter.hasNext()) {
      keys.add(iter.next().getName());
    }
    return keys;
  }

  private String getKeyName() {
    return keyNames.get(ThreadLocalRandom.current().nextInt(keyNames.size()));
  }

  public void printStats() {

  }

}

