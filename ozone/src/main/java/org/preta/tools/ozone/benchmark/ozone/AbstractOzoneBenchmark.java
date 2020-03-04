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

package org.preta.tools.ozone.benchmark.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.VolumeArgs;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.preta.tools.ozone.benchmark.IoStats;

import java.io.IOException;
import java.io.OutputStream;

public abstract class AbstractOzoneBenchmark implements Runnable {

  private OzoneConfiguration config;
  private IoStats ioStats;
  private OzoneClient client;

  public void run() {
    try {
      config = new OzoneConfiguration();
      client = OzoneClientFactory.getClient(config);
      addShutdownHook();
      ioStats = new IoStats();
      execute();
    } catch (IOException ex) {
      System.err.println("Got exception!");
      ex.printStackTrace();
    }
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        System.out.println("Final Stats!");
        printStats();
      } catch (Exception e) {
        System.err.println("Encountered Exception while benchmarking OzoneManager!");
        e.printStackTrace();
      }
    }));
  }

  OzoneConfiguration getConfig() {
    return config;
  }

  IoStats getIoStats() {
    return ioStats;
  }

  public abstract void execute();

  public abstract void printStats();

  protected OzoneClient getClient() {
    return client;
  }

  void createVolume(String user, String volume) throws IOException {
    try {
      client.getObjectStore().createVolume(volume, VolumeArgs.newBuilder()
          .setOwner(user)
          .setAdmin(user)
          .build());
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.VOLUME_ALREADY_EXISTS) {
        throw ex;
      }
    }
  }

  void createBucket(String volume, String bucket) throws IOException {
    try {
      client.getObjectStore().getVolume(volume).createBucket(bucket);
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.BUCKET_ALREADY_EXISTS) {
        throw ex;
      }
    }
  }


  void writeKey(OzoneBucket bucket, String key, byte[] data) {
    try {
      final long startTime = System.nanoTime();
      try (OutputStream stream = bucket.createKey(key, data.length)) {
        stream.write(data);
      }
      final long writeTime = System.nanoTime() - startTime;
      ioStats.addKeyWriteCpuTime(writeTime);
      ioStats.setMaxKeyWriteTime(writeTime);
      ioStats.incrKeysCreated();
    } catch (IOException ex) {
      System.err.println("Encountered Exception while creating key:");
      ex.printStackTrace();
    }
  }

}
