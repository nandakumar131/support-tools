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

package org.preta.tools.ozone.benchmark.om;

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
import org.apache.hadoop.ozone.om.helpers.*;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.preta.tools.ozone.benchmark.IoStats;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;

public abstract class AbstractOmBenchmark implements Runnable {

  private OzoneConfiguration config;
  private IoStats ioStats;
  private OzoneManagerProtocol client;

  public void run() {
    try {
      config = new OzoneConfiguration();
      ioStats = new IoStats();
      final long omVersion = RPC.getProtocolVersion(OzoneManagerProtocolPB.class);
      final InetSocketAddress omAddress = OmUtils.getOmAddressForClients(config);
      RPC.setProtocolEngine(config, OzoneManagerProtocolPB.class, ProtobufRpcEngine.class);
      client = new OzoneManagerProtocolClientSideTranslatorPB(
          RPC.getProxy(OzoneManagerProtocolPB.class, omVersion,
              omAddress, UserGroupInformation.getCurrentUser(), config,
              NetUtils.getDefaultSocketFactory(config),
              Client.getRpcTimeout(config)), "Ozone Manager Perf Test");

      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          System.err.println("Executing shutdown hook.");
        } catch (Exception e) {
          System.err.println("Encountered Exception while benchmarking OzoneManager!");
          e.printStackTrace();
        }
      }));
      execute();
    } catch (IOException ex) {
      System.err.println("Got exception!");
      ex.printStackTrace();
    }
  }

  OzoneConfiguration getConfig() {
    return config;
  }

  IoStats getIoStats() {
    return ioStats;
  }

  public abstract void execute();

  void createVolume(String user, String volume) throws IOException {
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

  void createBucket(String volume, String bucket) throws IOException {
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


  void writeKey(String volume, String bucket, String key) {
    try {
      final StorageSize blockSize = StorageSize.parse(OZONE_SCM_BLOCK_SIZE_DEFAULT);
      final long blockSizeInBytes = (long) blockSize.getUnit().toBytes(blockSize.getValue());
      final OmKeyArgs keyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volume)
          .setBucketName(bucket)
          .setKeyName(key)
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

  void readKey(String volume, String bucket, String key) {
    try {
      final OmKeyArgs keyArgs = new OmKeyArgs.Builder()
          .setVolumeName(volume)
          .setBucketName(bucket)
          .setKeyName(key)
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

}
