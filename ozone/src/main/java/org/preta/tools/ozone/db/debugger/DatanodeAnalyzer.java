package org.preta.tools.ozone.db.debugger;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.hdds.utils.MetadataKeyFilters;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.impl.ContainerDataYaml;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.interfaces.DBHandle;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.utils.ContainerInspectorUtil;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.helpers.BlockUtils;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerUtil;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DeleteTransactionStore;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V1;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V2;
import static org.apache.hadoop.ozone.OzoneConsts.SCHEMA_V3;

public class DatanodeAnalyzer {

  private final OzoneConfiguration config;
  private final String clusterID;
  private final File storageRoot;
  private final String storageID;
  private final ConcurrentSkipListMap<Long, Container<?>> containerMap = new
      ConcurrentSkipListMap<>();

  public DatanodeAnalyzer(String clusterID, File storageRoot, String storageID) {
    this.config = new OzoneConfiguration();
    this.clusterID = clusterID;
    this.storageRoot = storageRoot;
    this.storageID = storageID;

  }

  public void readVolume() {
    Preconditions.checkNotNull(storageRoot, "hddsVolumeRootDir" +
        "cannot be null");

    //filtering storage directory
    File[] storageDirs = storageRoot.listFiles(File::isDirectory);

    if (storageDirs == null || (storageDirs.length == 0)) {
      System.err.println(
          "IO error for the volume " + storageRoot + ", skipped loading.");
      System.exit(1);
    }

    File clusterIDDir = new File(storageRoot, clusterID);
    File idDir = clusterIDDir;
    if (storageDirs.length == 1 && !clusterIDDir.exists()) {
      // If the one directory is not the cluster ID directory, assume it is
      // the old SCM ID directory used before SCM HA.
      idDir = storageDirs[0];
    } else {
      // There are 1 or more storage directories. We only care about the
      // cluster ID directory.
      if (!clusterIDDir.exists()) {
        System.err.println("Volume " + storageRoot +
            " is in an inconsistent state. Expected " +
            "clusterID directory " + clusterID + " not found.");
        System.exit(1);
      }
    }

    System.out.println("Start to verify containers on volume " + storageRoot);
    File currentDir = new File(idDir, Storage.STORAGE_DIR_CURRENT);
    File[] containerTopDirs = currentDir.listFiles();
    if (containerTopDirs != null) {
      for (File containerTopDir : containerTopDirs) {
        if (containerTopDir.isDirectory()) {
          File[] containerDirs = containerTopDir.listFiles();
          if (containerDirs != null) {
            for (File containerDir : containerDirs) {
              try {
                File containerFile =
                    ContainerUtils.getContainerFile(containerDir);
                long containerID = ContainerUtils.getContainerID(containerDir);
                if (containerFile.exists()) {
                  verifyContainerFile(containerID, containerFile);
                } else {
                  System.err.println(
                      "Missing .container file for ContainerID: " +
                          containerDir.getName());
                }
              } catch (Throwable e) {
                System.err.println("Failed to load container from " +
                    containerDir.getAbsolutePath());
                e.printStackTrace();
              }
            }
          }
        }
      }
    }
    System.out.println("Finish verifying containers on volume  " + storageRoot);
  }

  private void verifyContainerFile(long containerID, File containerFile) {
    try {
      ContainerData containerData = ContainerDataYaml.readContainerFile(
          containerFile);
      if (containerID != containerData.getContainerID()) {
        System.err.println("Invalid ContainerID in file " + containerFile +
            ". Skipping loading of this container.");
        return;
      }
      verifyAndFixupContainerData(containerData);
    } catch (IOException ex) {
      System.err.println(
          "Failed to parse ContainerFile for ContainerID: " + containerID);
      ex.printStackTrace();
    }
  }

  public void verifyAndFixupContainerData(ContainerData containerData)
      throws IOException {
    switch (containerData.getContainerType()) {
    case KeyValueContainer:
      if (containerData instanceof KeyValueContainerData) {
        KeyValueContainerData kvContainerData = (KeyValueContainerData)
            containerData;
        parseKVContainerData(kvContainerData, config);
        KeyValueContainer kvContainer =
            new KeyValueContainer(kvContainerData, config);
        containerMap.put(containerData.getContainerID(), kvContainer);
      } else {
        throw new StorageContainerException("Container File is corrupted. " +
            "ContainerType is KeyValueContainer but cast to " +
            "KeyValueContainerData failed. ",
            ContainerProtos.Result.CONTAINER_METADATA_ERROR);
      }
      break;
    default:
      throw new StorageContainerException("Unrecognized ContainerType " +
          containerData.getContainerType(),
          ContainerProtos.Result.UNKNOWN_CONTAINER_TYPE);
    }
  }

  public void parseKVContainerData(KeyValueContainerData kvContainerData,
                                          ConfigurationSource config) throws IOException {

    long containerID = kvContainerData.getContainerID();

    // Verify Checksum
    ContainerUtils.verifyChecksum(kvContainerData, config);

    if (kvContainerData.getSchemaVersion() == null) {
      // If this container has not specified a schema version, it is in the old
      // format with one default column family.
      kvContainerData.setSchemaVersion(OzoneConsts.SCHEMA_V1);
    }

    File dbFile = getContainerDBFile(kvContainerData);
    if (!dbFile.exists()) {
      System.err.println("Container DB file is missing for ContainerID " + containerID + ". Skipping loading of this container.");
      // Don't further process this container, as it is missing db file.
      return;
    }
    kvContainerData.setDbFile(dbFile);

    DatanodeConfiguration dnConf =
        config.getObject(DatanodeConfiguration.class);
    boolean bCheckChunksFilePath = dnConf.getCheckEmptyContainerDir();

    if (kvContainerData.getSchemaVersion().equals(OzoneConsts.SCHEMA_V3)) {
      try (DBHandle db = BlockUtils.getDB(kvContainerData, config)) {
        populateContainerMetadata(kvContainerData,
            db.getStore(), bCheckChunksFilePath);
      }
      return;
    }

    DBHandle cachedDB = null;
    DatanodeStore store = null;
    try {
      try {
        boolean readOnly = ContainerInspectorUtil.isReadOnly(
            ContainerProtos.ContainerType.KeyValueContainer);
        store = BlockUtils.getUncachedDatanodeStore(
            kvContainerData, config, readOnly);
      } catch (IOException e) {
        // If an exception is thrown, then it may indicate the RocksDB is
        // already open in the container cache. As this code is only executed at
        // DN startup, this should only happen in the tests.
        cachedDB = BlockUtils.getDB(kvContainerData, config);
        store = cachedDB.getStore();
      }
      populateContainerMetadata(kvContainerData, store, bCheckChunksFilePath);
    } finally {
      if (cachedDB != null) {
        // If we get a cached instance, calling close simply decrements the
        // reference count.
        cachedDB.close();
      } else if (store != null) {
        // We only stop the store if cacheDB is null, as otherwise we would
        // close the rocksDB handle in the cache and the next reader would fail
        try {
          store.stop();
        } catch (IOException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException("Unexpected exception closing the " +
              "RocksDB when loading containers", e);
        }
      }
    }
  }

  public  File getContainerDBFile(KeyValueContainerData containerData) {
    if (containerData.getSchemaVersion().equals(OzoneConsts.SCHEMA_V3)) {
      System.out.println(new File(new File(storageRoot, storageID), OzoneConsts.CONTAINER_DB_NAME));
      return new File(new File(new File(storageRoot, clusterID), storageID), OzoneConsts.CONTAINER_DB_NAME);
    }
    return getContainerDBFile(containerData.getMetadataPath(), containerData);
  }

  public static File getContainerDBFile(String baseDir,
                                        KeyValueContainerData containerData) {
    return new File(baseDir, containerData.getContainerID() + OzoneConsts.DN_CONTAINER_DB);
  }

    private static void populateContainerMetadata(
      KeyValueContainerData kvContainerData, DatanodeStore store,
      boolean bCheckChunksFilePath)
      throws IOException {
    boolean isBlockMetadataSet = false;
    Table<String, Long> metadataTable = store.getMetadataTable();

    // Set pending deleted block count.
    Long pendingDeleteBlockCount =
        metadataTable.get(kvContainerData
            .getPendingDeleteBlockCountKey());
    if (pendingDeleteBlockCount != null) {
      kvContainerData.incrPendingDeletionBlocks(
          pendingDeleteBlockCount);
    } else {
      // Set pending deleted block count.
      MetadataKeyFilters.KeyPrefixFilter filter =
          kvContainerData.getDeletingBlockKeyFilter();
      int numPendingDeletionBlocks = store.getBlockDataTable()
          .getSequentialRangeKVs(kvContainerData.startKeyEmpty(),
              Integer.MAX_VALUE, kvContainerData.containerPrefix(),
              filter).size();
      kvContainerData.incrPendingDeletionBlocks(numPendingDeletionBlocks);
    }

    // Set delete transaction id.
    Long delTxnId =
        metadataTable.get(kvContainerData.getLatestDeleteTxnKey());
    if (delTxnId != null) {
      kvContainerData
          .updateDeleteTransactionId(delTxnId);
    }

    // Set BlockCommitSequenceId.
    Long bcsId = metadataTable.get(
        kvContainerData.getBcsIdKey());
    if (bcsId != null) {
      kvContainerData
          .updateBlockCommitSequenceId(bcsId);
    }

    // Set bytes used.
    // commitSpace for Open Containers relies on usedBytes
    Long bytesUsed =
        metadataTable.get(kvContainerData.getBytesUsedKey());
    if (bytesUsed != null) {
      isBlockMetadataSet = true;
      kvContainerData.setBytesUsed(bytesUsed);
    }

    // Set block count.
    Long blockCount = metadataTable.get(
        kvContainerData.getBlockCountKey());
    if (blockCount != null) {
      isBlockMetadataSet = true;
      kvContainerData.setBlockCount(blockCount);
    }

  }

  public long getTotalPendingBlocksToDelete() {
    long totalPendingBlocks = 0;
    for (Container container : containerMap.values()) {
      KeyValueContainerData containerData = (KeyValueContainerData)container.getContainerData();
      if (containerData.isClosed()) {
        totalPendingBlocks = totalPendingBlocks + containerData.getNumPendingDeletionBlocks();
      }
    }
    return totalPendingBlocks;
  }

  private void cleanupPendingDeleteBlocks() throws StorageContainerException {
    for (Container container : containerMap.values()) {
      KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
      File dataDir = new File(containerData.getChunksPath());
      long deletedCount;
      try (DBHandle meta = BlockUtils.getDB(containerData, config)) {
        if (containerData.getSchemaVersion().equals(SCHEMA_V1)) {
          deletedCount = deleteViaSchema1(meta, container, dataDir);
        } else if (containerData.getSchemaVersion().equals(SCHEMA_V2)) {
          deletedCount = deleteViaSchema2(meta, container, dataDir);
        } else if (containerData.getSchemaVersion().equals(SCHEMA_V3)) {
          deletedCount = deleteViaSchema3(meta, container, dataDir);
        } else {
          throw new UnsupportedOperationException("Only schema version 1,2,3 are supported.");
        }
        System.out.println("Deleted " + deletedCount + " blocks in container " + containerData.getContainerID());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public boolean checkDataDir(File dataDir) {
    boolean b = true;
    if (!dataDir.exists() || !dataDir.isDirectory()) {
      System.err.println("Invalid container data dir " + dataDir.getAbsolutePath() + " : does not exist or not a directory");
      b = false;
    }
    return b;
  }

  public  long deleteViaSchema1(DBHandle meta, Container container, File dataDir) throws IOException {
    if (!checkDataDir(dataDir)) {
      return 0L;
    }
    try {
      Table<String, BlockData> blockDataTable = meta.getStore().getBlockDataTable();
      KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
      long containerID = containerData.getContainerID();
      MetadataKeyFilters.KeyPrefixFilter filter = containerData.getDeletingBlockKeyFilter();
      List<? extends Table.KeyValue<String, BlockData>> toDeleteBlocks =
          blockDataTable.getSequentialRangeKVs(containerData.startKeyEmpty(),
                    Integer.MAX_VALUE, containerData.containerPrefix(),
                    filter);
        if (toDeleteBlocks.isEmpty()) {
          System.out.println("No under deletion block found in container : " + containerData.getContainerID());
          return 0L;
        }

      List<String> succeedBlocks = new LinkedList<>();
      System.out.println("Container : " + containerID + "To-Delete blocks : " + toDeleteBlocks.size());

      Handler handler = new KeyValueHandler(config, null, null, null, null, null);

        long releasedBytes = 0;
        for (Table.KeyValue<String, BlockData> entry: toDeleteBlocks) {
          String blockName = entry.getKey();
          if (entry.getValue() == null) {
            System.err.println("Missing delete block(Container = " +
                container.getContainerData().getContainerID() + ", Block = " +
                blockName);
            continue;
          }
          try {
            handler.deleteBlock(container, entry.getValue());
            releasedBytes += KeyValueContainerUtil.getBlockLength(
                entry.getValue());
            succeedBlocks.add(blockName);
          } catch (InvalidProtocolBufferException e) {
            System.err.println("Failed to parse block info for block " + blockName);
            e.printStackTrace();
          } catch (IOException e) {
            System.err.println("Failed to delete files for block " + blockName);
            e.printStackTrace();
          }
        }

        // Once chunks in the blocks are deleted... remove the blockID from
        // blockDataTable.
        try (BatchOperation batch = meta.getStore().getBatchHandler()
            .initBatchOperation()) {
          for (String entry : succeedBlocks) {
            blockDataTable.deleteWithBatch(batch, entry);
          }

          // Handler.deleteBlock calls deleteChunk to delete all the chunks
          // in the block. The ContainerData stats (DB and in-memory) are not
          // updated with decremented used bytes during deleteChunk. This is
          // done here so that all the DB update for block delete can be
          // batched together while committing to DB.
          int deletedBlocksCount = succeedBlocks.size();
          containerData.updateAndCommitDBCounters(meta, batch,
              deletedBlocksCount, releasedBytes);
          // update count of pending deletion blocks, block count and used
          // bytes in in-memory container status.
          containerData.decrPendingDeletionBlocks(deletedBlocksCount);
          containerData.decrBlockCount(deletedBlocksCount);
          containerData.decrBytesUsed(releasedBytes);
        }

        if (!succeedBlocks.isEmpty()) {
          System.out.println("Container: " + containerData.getContainerID() + " ,deleted blocks: " + succeedBlocks.size() +
              " space reclaimed: " + releasedBytes + ".");
        }
        return succeedBlocks.size();
      } catch (IOException exception) {
      System.err.println("Deletion operation was not successful for container: " + container.getContainerData().getContainerID());
      exception.printStackTrace();
      throw exception;
      }
    }

    public long deleteViaSchema2(
        DBHandle meta, Container container, File dataDir) throws IOException {
      Deleter schema2Deleter = (table, batch, tid) -> {
        Table<Long, DeletedBlocksTransaction> delTxTable = (Table<Long, DeletedBlocksTransaction>) table;
        delTxTable.deleteWithBatch(batch, tid);
      };
      Table<Long, DeletedBlocksTransaction> deleteTxns = ((DeleteTransactionStore<Long>) meta.getStore()).getDeleteTransactionTable();
      try (TableIterator<Long, ? extends Table.KeyValue<Long, DeletedBlocksTransaction>> iterator = deleteTxns.iterator()) {
        return deleteViaTransactionStore(iterator, meta, container, dataDir,  schema2Deleter);
      }
    }


  public long deleteViaSchema3(
      DBHandle meta, Container container, File dataDir) throws IOException {
    KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
    Deleter schema3Deleter = (table, batch, tid) -> {
      Table<String, DeletedBlocksTransaction> delTxTable = (Table<String, DeletedBlocksTransaction>) table;
      delTxTable.deleteWithBatch(batch,
          containerData.getDeleteTxnKey(tid));
    };
    Table<String, DeletedBlocksTransaction> deleteTxns = ((DeleteTransactionStore<String>) meta.getStore())
        .getDeleteTransactionTable();
    try (TableIterator<String,
        ? extends Table.KeyValue<String, DeletedBlocksTransaction>>
             iterator = deleteTxns.iterator(containerData.containerPrefix())) {
      return deleteViaTransactionStore(iterator, meta, container, dataDir,  schema3Deleter);
    }
  }

  private interface Deleter {
    void apply(Table<?, DeletedBlocksTransaction> deleteTxnsTable,
               BatchOperation batch, long txnID) throws IOException;
  }

  private long deleteViaTransactionStore(
      TableIterator<?, ? extends Table.KeyValue<?, DeletedBlocksTransaction>> iter, DBHandle meta,
      Container container, File dataDir, Deleter deleter) throws IOException {
    if (!checkDataDir(dataDir)) {
      return 0L;
    }
    try {
      KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
      Table<String, BlockData> blockDataTable = meta.getStore().getBlockDataTable();
      DeleteTransactionStore<?> txnStore = (DeleteTransactionStore<?>) meta.getStore();
      Table<?, DeletedBlocksTransaction> deleteTxns = txnStore.getDeleteTransactionTable();
      List<DeletedBlocksTransaction> delBlocks = new ArrayList<>();
      while (iter.hasNext()) {
        DeletedBlocksTransaction delTx = iter.next().getValue();
        delBlocks.add(delTx);
      }
      if (delBlocks.isEmpty()) {
        containerData.resetPendingDeleteBlockCount(meta);
        return 0L;
      }

      System.out.println("Container : " + containerData.getContainerID() + ", To-Delete blocks : " + delBlocks.size());

      Handler handler = new KeyValueHandler(config, null, null, null, null, null);

      DeleteTransactionStats deleteBlocksResult = deleteTransactions(delBlocks, handler, blockDataTable, container);
      int deletedBlocksProcessed = deleteBlocksResult.getBlocksProcessed();
      int deletedBlocksCount = deleteBlocksResult.getBlocksDeleted();
      long releasedBytes = deleteBlocksResult.getBytesReleased();

      // Once blocks are deleted... remove the blockID from blockDataTable
      // and also remove the transactions from txnTable.
      try (BatchOperation batch = meta.getStore().getBatchHandler()
          .initBatchOperation()) {
        for (DeletedBlocksTransaction delTx : delBlocks) {
          deleter.apply(deleteTxns, batch, delTx.getTxID());
          for (Long blk : delTx.getLocalIDList()) {
            blockDataTable.deleteWithBatch(batch,
                containerData.getBlockKey(blk));
          }
        }

        // Handler.deleteBlock calls deleteChunk to delete all the chunks
        // in the block. The ContainerData stats (DB and in-memory) are not
        // updated with decremented used bytes during deleteChunk. This is
        // done here so that all the DB updates for block delete can be
        // batched together while committing to DB.
        containerData.updateAndCommitDBCounters(meta, batch,
            deletedBlocksCount, releasedBytes);

        // update count of pending deletion blocks, block count and used
        // bytes in in-memory container status and used space in volume.
        containerData.decrPendingDeletionBlocks(deletedBlocksProcessed);
        containerData.decrBlockCount(deletedBlocksCount);
        containerData.decrBytesUsed(releasedBytes);
      }

      System.out.println("Container: " + containerData.getContainerID() + " ,deleted blocks: " + deletedBlocksCount +
          " space reclaimed: " + releasedBytes + ".");

      return deletedBlocksProcessed;
    } catch (IOException exception) {
      System.err.println("Deletion operation was not successful for container: " + container.getContainerData().getContainerID());
      exception.printStackTrace();
      throw exception;
    }
  }

  private DeleteTransactionStats deleteTransactions(List<DeletedBlocksTransaction> delBlocks, Handler handler,
                                  Table<String, BlockData> blockDataTable, Container container)
      throws IOException {
    int blocksProcessed = 0;
    int blocksDeleted = 0;
    long bytesReleased = 0;
    KeyValueContainerData containerData = (KeyValueContainerData) container.getContainerData();
    for (DeletedBlocksTransaction entry : delBlocks) {
      for (Long blkLong : entry.getLocalIDList()) {
        String blk = containerData.getBlockKey(blkLong);
        BlockData blkInfo = blockDataTable.get(blk);
        if (blkInfo == null) {
          try {
            handler.deleteUnreferenced(container, blkLong);
          } catch (IOException e) {
            System.err.println("Failed to delete files for unreferenced block " + blkLong + " of" +
                    " container " + container.getContainerData().getContainerID());
            e.printStackTrace();
          } finally {
            blocksProcessed++;
          }
          continue;
        }

        boolean deleted = false;
        try {
          handler.deleteBlock(container, blkInfo);
          blocksDeleted++;
          deleted = true;
        } catch (IOException e) {
          // TODO: if deletion of certain block retries exceed the certain
          //  number of times, service should skip deleting it,
          //  otherwise invalid numPendingDeletionBlocks could accumulate
          //  beyond the limit and the following deletion will stop.
          System.err.println("Failed to delete files for block " + blkLong);
          e.printStackTrace();
        } finally {
          blocksProcessed++;
        }

        if (deleted) {
          try {
            bytesReleased += KeyValueContainerUtil.getBlockLength(blkInfo);
          } catch (IOException e) {
            System.err.println("Failed to delete files for block " + blkLong);
            e.printStackTrace();          }
        }
      }
    }
    return new DeleteTransactionStats(blocksProcessed,
        blocksDeleted, bytesReleased);
  }

  private static class DeleteTransactionStats {
    private final int blocksProcessed;
    private final int blocksDeleted;
    private final long bytesReleased;

    DeleteTransactionStats(int blocksProcessed, int blocksDeleted, long bytesReleased) {
      this.blocksProcessed = blocksProcessed;
      this.blocksDeleted = blocksDeleted;
      this.bytesReleased = bytesReleased;
    }

    int getBlocksProcessed() {
      return blocksProcessed;
    }

    int getBlocksDeleted() {
      return blocksDeleted;
    }

    long getBytesReleased() {
      return bytesReleased;
    }
  }

  public static void main(String[] args) throws Exception {
     DatanodeAnalyzer analyzer = new DatanodeAnalyzer(args[0], new File(args[1]), args[2]);
     analyzer.readVolume();
     System.out.println("Total pending blocks to delete: " + analyzer.getTotalPendingBlocksToDelete());
     if (args[3].equals("cleanup")) {
       analyzer.cleanupPendingDeleteBlocks();
     }

  }

}
