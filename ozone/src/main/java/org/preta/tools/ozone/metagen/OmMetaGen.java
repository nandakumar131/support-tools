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

package org.preta.tools.ozone.metagen;

import org.apache.hadoop.conf.StorageSize;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.UserVolumeInfo;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.hdds.utils.UniqueId;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.preta.tools.ozone.OzoneVersionProvider;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT;

@Command(name="om",
    description = "Generate OzoneManager metadata.",
    versionProvider = OzoneVersionProvider.class,
    mixinStandardHelpOptions = true)
public class OmMetaGen implements Runnable {

  @Option(names = {"-p", "--dbPath"},
      description = "Path to generate OzoneManager db.")
  private String path;

  @Option(names = {"-u", "--user"},
      description = "User Name.")
  private String user;

  @Option(names = {"-v", "--volume"},
      description = "Ozone Volume.")
  private String volume;

  @Option(names = {"-b", "--bucket"},
      description = "Ozone Bucket.")
  private String bucket;

  @Option(names = {"-c", "--count"},
      description = "Number of keys to create.")
  private int count;

  @Option(names = {"-l", "--blocks"},
      description = "Number of blocks per key.")
  private int blocks;

  public OmMetaGen() {
    this.path = ".";
    this.user = "admin";
    this.volume = "instagram";
    this.bucket = "images";
    this.count = 1000000000;
    this.blocks = -1;
  }

  @Override
  public void run() {
    try {
      System.out.println("Starting OmMetaGen.");
      final OzoneConfiguration conf = getConfiguration();
      final OMMetadataManager metadataManager = new OmMetadataManagerImpl(conf);
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          System.err.println("Executing shutdown hook.");
          metadataManager.stop();
        } catch (Exception e) {
          System.err.println("Encountered Exception while closing db!");
          e.printStackTrace();
        }
      }));
      try {
        createVolume(metadataManager);
        createBucket(metadataManager);
        createKeys(metadataManager);
      } finally {
        System.out.println("Stopping OmMetaGen.");
        metadataManager.stop();
      }
    } catch (Exception ex) {
      System.err.println("Encountered Exception:");
      ex.printStackTrace();
    }
  }

  private OzoneConfiguration getConfiguration() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, path);
    return conf;
  }

  private void createVolume(final OMMetadataManager metadataManager) throws IOException  {
    final Table<String, UserVolumeInfo> userTable = metadataManager.getUserTable();
    final Table<String, OmVolumeArgs> volumeTable = metadataManager.getVolumeTable();
    final String userKey = metadataManager.getUserKey(user);
    final String volumeKey = metadataManager.getVolumeKey(volume);
    final OmVolumeArgs volumeArgs = OmVolumeArgs.newBuilder()
        .setVolume(volume)
        .setAdminName(user)
        .setOwnerName(user)
        .setQuotaInBytes(OzoneConsts.MAX_QUOTA_IN_BYTES)
        .setCreationTime(System.currentTimeMillis())
        .addOzoneAcls(OzoneAcl.toProtobuf(OzoneAcl.parseAcl("user:"+user+":rw")))
        .build();
    if (!volumeTable.isExist(volumeKey)) {
      final List<String> volumeList = new ArrayList<>();
      if (userTable.isExist(userKey)) {
        volumeList.addAll(userTable.get(userKey).getVolumeNamesList());
      }
      volumeList.add(volume);
      userTable.put(userKey, UserVolumeInfo.newBuilder().addAllVolumeNames(volumeList).build());
      volumeTable.put(volumeKey, volumeArgs);
    }
  }

  private void createBucket(final OMMetadataManager metadataManager) throws IOException {
    final Table<String, OmBucketInfo> bucketTable = metadataManager.getBucketTable();
    final String bucketKey = metadataManager.getBucketKey(volume, bucket);
    final OmBucketInfo bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volume)
        .setBucketName(bucket)
        .setIsVersionEnabled(false)
        .setStorageType(StorageType.DISK)
        .setAcls(Collections.singletonList(OzoneAcl.parseAcl("user:"+user+":rw")))
        .build();
    if (!bucketTable.isExist(bucketKey)) {
      bucketTable.put(bucketKey, bucketInfo);
    }
  }

  private void createKeys(OMMetadataManager metadataManager) throws IOException {
    final DBStore store = metadataManager.getStore();
    final Table<String, OmKeyInfo> keyTable = metadataManager.getKeyTable();
    final StorageSize blockSize = StorageSize.parse(OZONE_SCM_BLOCK_SIZE_DEFAULT);
    final long blockSizeInBytes = (long) blockSize.getUnit().toBytes(blockSize.getValue());
    final Map<Integer, Pipeline> pipelineCache = new HashMap<>();
    BatchOperation batch = store.initBatchOperation();
    for (int i = 0; i < count; i++){
      final String key = UUID.randomUUID().toString();
      final String ozoneKey = metadataManager.getOzoneKey(volume, bucket, key);
      final int magic = i % 5;
      final int numBlocks = blocks == -1 ? magic : blocks;
      final List<OmKeyLocationInfo> locations = new ArrayList<>(numBlocks);
      final Pipeline pipeline = pipelineCache.computeIfAbsent(magic,
          id -> Pipeline.newBuilder()
              .setId(PipelineID.randomId())
              .setType(ReplicationType.RATIS)
              .setFactor(ReplicationFactor.THREE)
              .setState(Pipeline.PipelineState.OPEN)
              .setNodes(new ArrayList<DatanodeDetails>() {
                {
                  add(getRandomDatanode());
                  add(getRandomDatanode());
                  add(getRandomDatanode());
                }
              })
              .build());
      for (int loc = 0; loc < numBlocks; loc++) {
        final OmKeyLocationInfo.Builder builder = new OmKeyLocationInfo.Builder()
            .setBlockID(new BlockID(new ContainerBlockID(magic + 1, UniqueId.next())))
            .setLength(blockSizeInBytes)
            .setOffset(0)
            .setPipeline(pipeline);
        locations.add(builder.build());
      }
      final OmKeyInfo.Builder builder = new OmKeyInfo.Builder()
          .setVolumeName(volume)
          .setBucketName(bucket)
          .setKeyName(key)
          .setReplicationType(ReplicationType.RATIS)
          .setReplicationFactor(ReplicationFactor.THREE)
          .setAcls(Collections.singletonList(OzoneAcl.parseAcl("user:"+user+":rw")))
          .setOmKeyLocationInfos(Collections.singletonList(
              new OmKeyLocationInfoGroup(0, locations)))
          .setCreationTime(Time.now())
          .setModificationTime(Time.now())
          .setDataSize(blockSizeInBytes * numBlocks);
      keyTable.putWithBatch(batch, ozoneKey, builder.build());
      if (i % 100000 == 0) {
        store.commitBatchOperation(batch);
        batch.close();
        batch = store.initBatchOperation();
        System.out.print('\r');
        System.out.print(i + " / " + count);
      }
    }
    store.commitBatchOperation(batch);
    batch.close();
    System.out.print('\r');
    System.out.print(count + " / " + count);
    System.out.println();
  }

  private DatanodeDetails getRandomDatanode() {
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    String ipAddress = random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256)
        + "." + random.nextInt(256);

    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID().toString())
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort)
        .setNetworkLocation("/default");
    return builder.build();
  }

}
