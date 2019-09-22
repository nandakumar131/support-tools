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

import org.apache.hadoop.conf.StorageSize;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.preta.tools.ozone.OzoneVersionProvider;
import org.preta.tools.ozone.ReadableTimestampConverter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;


@Command(name="om",
    description = "Benchmark OzoneManager.",
    versionProvider = OzoneVersionProvider.class,
    mixinStandardHelpOptions = true)
public class BenchmarkOM implements Runnable {

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

  @Option(names = {"-w", "--numWriteThreads"},
      description = "Number of writer threads.")
  private int writerThreads;

  @Option(names = {"-r", "--numReaderThreads"},
      description = "Number of reader threads.")
  private int readerThreads;

  private final IoStats ioStats;
  private final String keyNamePrefix;
  private final AtomicLong writeKeyNamePointer;
  private final AtomicLong readKeyNamePointer;

  private OzoneManagerProtocolClientSideTranslatorPB client;

  public BenchmarkOM() {
    this.user = "admin";
    this.volume = "instagram";
    this.bucket = "images";
    this.writerThreads = 10;
    this.readerThreads = 10;
    this.ioStats = new IoStats();
    this.keyNamePrefix = UUID.randomUUID().toString() + "-" + System.nanoTime();
    this.writeKeyNamePointer = new AtomicLong(0);
    this.readKeyNamePointer = new AtomicLong(0);
  }

  @Override
  public void run() {
    try {
      System.out.println("Benchmarking OzoneManager.");
      final OzoneConfiguration conf = getConfiguration();
      final long omVersion = RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
      final InetSocketAddress omAddress = OmUtils.getOmAddressForClients(conf);
      RPC.setProtocolEngine(conf, OzoneManagerProtocolPB.class, ProtobufRpcEngine.class);
      this.client = new OzoneManagerProtocolClientSideTranslatorPB(
          RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
              omAddress, UserGroupInformation.getCurrentUser(), conf,
              NetUtils.getDefaultSocketFactory(conf),
              Client.getRpcTimeout(conf)), "Ozone Manager Perf Test");

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          System.err.println("Executing shutdown hook.");
        } catch (Exception e) {
          System.err.println("Encountered Exception while benchmarking OzoneManager!");
          e.printStackTrace();
        }
      }));

      try {
        createVolume();
        createBucket();
        final long runtimeInNs = runtime * 1_000_000_000;
        final long endTimeInNs = System.nanoTime() + runtimeInNs;
        final ExecutorService writeExecutor = Executors.newFixedThreadPool(writerThreads);
        final ExecutorService readExecutor = Executors.newFixedThreadPool(readerThreads);

        // Warming up!
        for (int i = 0; i < 100; i++) {
          writeKey();
        }

        for (int i = 0; i < writerThreads; i++) {
          writeExecutor.submit(() -> {
            while (System.nanoTime() < endTimeInNs) {
              writeKey();
            }
          });
        }

        for (int i = 0; i < readerThreads; i++) {
          readExecutor.submit(() -> {
            while (System.nanoTime() < endTimeInNs) {
              readKey();
            }
          });
        }

        writeExecutor.shutdown();
        readExecutor.shutdown();
        writeExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        readExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
      } finally {
        System.out.println(ioStats);
        System.out.println("Stopping...");
      }
    } catch (Exception ex) {
      System.err.println("Encountered Exception:");
      ex.printStackTrace();
    }
  }


  private OzoneConfiguration getConfiguration() {
    return new OzoneConfiguration();
  }

  private void createVolume() throws IOException {
    try {
      client.createVolume(OmVolumeArgs.newBuilder()
          .setVolume(volume)
          .setAdminName(user)
          .setOwnerName(user)
          .setQuotaInBytes(OzoneConsts.MAX_QUOTA_IN_BYTES)
          .build());
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.VOLUME_ALREADY_EXISTS) {
        throw ex;
      }
    }
  }

  private void createBucket() throws IOException {
    try {
      client.createBucket(OmBucketInfo.newBuilder()
          .setVolumeName(volume)
          .setBucketName(bucket)
          .setIsVersionEnabled(false)
          .setStorageType(StorageType.DEFAULT)
          .setAcls(Collections.emptyList())
          .build());
    } catch (OMException ex) {
      if (ex.getResult() != OMException.ResultCodes.BUCKET_ALREADY_EXISTS) {
        throw ex;
      }
    }
  }

  private void writeKey() {
    try {
      final StorageSize blockSize = StorageSize.parse(OZONE_SCM_BLOCK_SIZE_DEFAULT);
      final long blockSizeInBytes = (long) blockSize.getUnit().toBytes(blockSize.getValue());
      final OmKeyArgs keyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volume)
          .setBucketName(bucket)
          .setKeyName(getKeyNameToWrite())
          .setDataSize(blockSizeInBytes)
          .setType(HddsProtos.ReplicationType.RATIS)
          .setFactor(HddsProtos.ReplicationFactor.THREE)
          .build();
      final OpenKeySession keySession = client.openKey(keyArgs);
      keyArgs.setLocationInfoList(keySession.getKeyInfo()
          .getLatestVersionLocations().getLocationList());
      final long clientId = keySession.getId();
      keyArgs.setDataSize(blockSizeInBytes);
      client.commitKey(keyArgs, clientId);
      ioStats.incrKeysCreated();
    } catch (IOException ex) {
      System.err.println("Encountered Exception while creating key:");
      ex.printStackTrace();
    }
  }

  private void readKey() {
    try {
      final OmKeyArgs keyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volume)
          .setBucketName(bucket)
          .setKeyName(getKeyNameToRead())
          .setType(HddsProtos.ReplicationType.RATIS)
          .setFactor(HddsProtos.ReplicationFactor.THREE)
          .build();
      final OmKeyInfo keyInfo = client.lookupKey(keyArgs);
      assert keyInfo != null;
      ioStats.incrKeysRead();
    } catch (IOException ex) {
      System.err.println("Encountered Exception while reading key:");
      ex.printStackTrace();
    }
  }

  private synchronized String getKeyNameToWrite() {
    return keyNamePrefix + "-" + writeKeyNamePointer.incrementAndGet();
  }

  private synchronized String getKeyNameToRead() {
    if (readKeyNamePointer.get() >= ioStats.getKeysCreated()) {
      readKeyNamePointer.set(0);
    }
    return keyNamePrefix + "-" + readKeyNamePointer.incrementAndGet();
  }

}
