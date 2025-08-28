package org.preta.tools.ozone.db.debugger;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMStorage;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.hadoop.ozone.OzoneConsts.OM_DB_NAME;
import static org.apache.hadoop.ozone.OzoneConsts.SNAPSHOT_INFO_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.BUCKET_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DELETED_DIR_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.DIRECTORY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.KEY_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.VOLUME_TABLE;

public class OMDBAnalyzer {

  private final OzoneConfiguration conf;
  private DBStore store;

  private Table<String, OmVolumeArgs> volumeTable;
  private Table<String, OmBucketInfo> bucketTable;
  private Table<String, OmKeyInfo> keyTable;
  private Table<String, OmKeyInfo> fileTable;
  private Table<String, OmKeyInfo> openKeyTable;
  private Table<String, OmKeyInfo> openFileTable;
  private Table<String, OmDirectoryInfo> directoryTable;
  private Table<String, OmKeyInfo> deletedDirectoryTable;
  private Table<String, TransactionInfo> transactionInfo;
  private Table<String, SnapshotInfo> snapshotInfoTable;

  private HashMap<Long, String> volumeMap = new HashMap<>();
  // Bucket name will be /volume/bucket
  private HashMap<Long, String> bucketMap = new HashMap<>();
  // object ID -> Parent ID
  private HashMap<Long, Long> directoryToParentMap = new HashMap<>();
  private HashMap<Long, String> directoryObjectToNameMap = new HashMap<>();

  private HashMap<Long, Long> deletedDirectoryToParentMap = new HashMap<>();
  private HashMap<Long, String> deletedDirectoryObjectToNameMap = new HashMap<>();
  private HashMap<Long, String> deletedDirectoryObjectToPathMap = new HashMap<>();

  private Map<Long, Long> deletedDirectoriesSize = new HashMap<>();

  private HashMap<String, String> bucketType = new HashMap<>();
  private HashMap<String, Long> bucketSize = new HashMap<>();

  private HashMap<String, Long> pendingForDeletion = new HashMap<>();

  public OMDBAnalyzer(String dbPath) throws Exception {
    this.conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbPath);
    loadDB();
    loadBuckets();
    loadDirectories();
    loadDeletedDirectories();
    constructDeletedDirectoryPath();
  }

  private void loadDB() throws Exception {
    final File metaDir = OMStorage.getOmDbDir(conf);
    store = OmMetadataManagerImpl.loadDB(conf, metaDir, OM_DB_NAME, false,
        java.util.Optional.of(true), Optional.empty(), false, false);
    volumeTable = store.getTable(VOLUME_TABLE, String.class, OmVolumeArgs.class);
    bucketTable = store.getTable(BUCKET_TABLE, String.class, OmBucketInfo.class);
    keyTable = this.store.getTable(KEY_TABLE, String.class, OmKeyInfo.class);
    fileTable = this.store.getTable(FILE_TABLE, String.class, OmKeyInfo.class);
    directoryTable = store.getTable(DIRECTORY_TABLE, String.class, OmDirectoryInfo.class);
    deletedDirectoryTable = store.getTable(DELETED_DIR_TABLE, String.class, OmKeyInfo.class);
    snapshotInfoTable = store.getTable(SNAPSHOT_INFO_TABLE, String.class, SnapshotInfo.class);
  }

  private void loadBuckets() throws IOException {
    Iterator<? extends Table.KeyValue<String, OmBucketInfo>> bucketIter = bucketTable.iterator();
    while (bucketIter.hasNext()) {
      Table.KeyValue<String, OmBucketInfo> data = bucketIter.next();
      String bucket = data.getKey();
      OmBucketInfo bucketInfo = data.getValue();
      bucketMap.put(bucketInfo.getObjectID(), bucket);
      bucketSize.put(bucket, 0L);
      pendingForDeletion.put(bucket, 0L);
      bucketType.put(bucket, bucketInfo.getBucketLayout().toString());
    }
  }

  private void loadDirectories() throws IOException {
    Iterator<? extends Table.KeyValue<String, OmDirectoryInfo>> tableIter = directoryTable.iterator();
    while (tableIter.hasNext()) {
      final Table.KeyValue<String, OmDirectoryInfo> data = tableIter.next();
      final OmDirectoryInfo directoryInfo = data.getValue();
      directoryToParentMap.put(directoryInfo.getObjectID(), directoryInfo.getParentObjectID());
      directoryObjectToNameMap.put(directoryInfo.getObjectID(), directoryInfo.getName());
    }
    System.out.println("Loaded " + directoryToParentMap.size() + " directories.");
  }

  private void loadDeletedDirectories() throws IOException {
    Iterator<? extends Table.KeyValue<String, OmKeyInfo>> tableIter = deletedDirectoryTable.iterator();
    while (tableIter.hasNext()) {
      final Table.KeyValue<String, OmKeyInfo> data = tableIter.next();
      final OmKeyInfo directoryInfo = data.getValue();
      deletedDirectoryToParentMap.put(directoryInfo.getObjectID(), directoryInfo.getParentObjectID());
      deletedDirectoryObjectToNameMap.put(directoryInfo.getObjectID(), directoryInfo.getFileName());
    }
  }

  private void constructDeletedDirectoryPath() {
    for (Long deletedDir : deletedDirectoryObjectToNameMap.keySet()) {
      deletedDirectoryObjectToPathMap.put(deletedDir,
          getPath(deletedDirectoryToParentMap.get(deletedDir)) + "/" +
              deletedDirectoryObjectToNameMap.get(deletedDir));
    }
  }


  private void calculateOBSBucketSize() throws IOException {
    Iterator<? extends Table.KeyValue<String, OmKeyInfo>> keyIterator = keyTable.iterator();
    while (keyIterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> data = keyIterator.next();
      String[] parts = data.getKey().split("/");
      String volume = parts[1];
      String bucket = parts[2];
      OmKeyInfo keyInfo = data.getValue();
      //long size = keyInfo.getDataSize();
      long size = getFileSize(keyInfo);
      String key = "/" + volume + "/" + bucket;
      long total = bucketSize.get(key) + size;
      bucketSize.put(key, total);
    }
  }

  private void calculateFSOBucketSize() throws IOException {
    Iterator<? extends Table.KeyValue<String, OmKeyInfo>> fileTableIterator = fileTable.iterator();
    while (fileTableIterator.hasNext()) {
      Table.KeyValue<String, OmKeyInfo> data = fileTableIterator.next();
      String[] parts = data.getKey().split("/");
      String volume = parts[1];
      String bucket = parts[2];
      OmKeyInfo keyInfo = data.getValue();
      boolean notOrphaned = isNotOrphan(keyInfo.getParentObjectID());
      //long size = keyInfo.getDataSize();
      long size = getFileSize(keyInfo);
      long bucketID = Long.parseLong(bucket);
      String key = bucketMap.get(bucketID);
      if (notOrphaned) {
        long total = bucketSize.get(key) + size;
        bucketSize.put(key, total);
      } else {
        long total = pendingForDeletion.get(key) + size;
        pendingForDeletion.put(key, total);
        Long missingParent = getMissingParentOfOrphanFile(keyInfo.getParentObjectID());
        if (!deletedDirectoryObjectToPathMap.containsKey(missingParent)) {
        }

        if (!deletedDirectoriesSize.containsKey(missingParent)) {
          deletedDirectoriesSize.put(missingParent, size);
        } else {
          long directoryTotal = deletedDirectoriesSize.get(missingParent) + size;
          deletedDirectoriesSize.put(missingParent, directoryTotal);
        }
      }
    }
  }

  private boolean isNotOrphan(long objectID) {
    if (bucketMap.containsKey(objectID)) {
      return true;
    }
    return directoryToParentMap.containsKey(objectID) &&
        isNotOrphan(directoryToParentMap.get(objectID));
  }

  private Long getMissingParentOfOrphanFile(long objectID) {
    if(directoryToParentMap.containsKey(objectID)) {
      return getMissingParentOfOrphanFile(directoryToParentMap.get(objectID));
    }
    return objectID;
  }

  private String getPath(long objectID) {
    if (bucketMap.containsKey(objectID)) {
      return bucketMap.get(objectID);
    }
    if (directoryObjectToNameMap.containsKey(objectID)) {
      return getPath(directoryToParentMap.get(objectID)) + "/" + directoryObjectToNameMap.get(objectID);
    }
    if (deletedDirectoryObjectToNameMap.containsKey(objectID)) {
      return getPath(deletedDirectoryToParentMap.get(objectID)) + "/" + deletedDirectoryObjectToNameMap.get(objectID);
    }
    return "[NOT FOUND]";
  }

  private long getFileSize(OmKeyInfo keyInfo) {
    long size = 0L;
    List<OmKeyLocationInfoGroup> groups = keyInfo.getKeyLocationVersions();
    for (OmKeyLocationInfoGroup group : groups) {
      List<OmKeyLocationInfo> locations = group.getLocationList();
      for (OmKeyLocationInfo location : locations) {
        size += location.getLength();
      }
    }
    return size;
  }

  private void printBucketSizes() {
    System.out.println("Bucket level info");
    long total = 0;
    long pendingForDelete = 0;
    for (String bucket : bucketSize.keySet()) {
      total += bucketSize.get(bucket);
      pendingForDelete += pendingForDeletion.get(bucket);
      if (bucketSize.get(bucket) > 1000000000){
        System.out.println(bucket + "(" + bucketType.get(bucket) + ")" + " - Size: " + (bucketSize.get(bucket)/1000000000) + " GB, " +
            "Pending for deletion: " + (pendingForDeletion.get(bucket)/1000000000) + " GB");
      }
    }
    System.out.println("Total: " + (total / 1000000000) + " GB");
    System.out.println("Pending for delete: " + (pendingForDelete  / 1000000000) + " GB");
  }


  private void printDeletedDirectorySizes() {
    System.out.println("Directory level info");
    long total = 0;
    long missingNameSize = 0;
    long missingNameCount = 0;

    for (Long deletedDir : deletedDirectoriesSize.keySet()) {
      String name = deletedDirectoryObjectToPathMap.get(deletedDir);
      long size = deletedDirectoriesSize.get(deletedDir);
      total += size;
      if (name == null) {
        missingNameSize += size;
        missingNameCount++;
      } else {
        if (size > 1000000000) {
          System.out.println(name + " - Size: " + (size / 1000000000) + " GB");
        }
      }
    }
    System.out.println("Missing parent directory, Size: " + (missingNameSize/1000000000) + " GB");
    System.out.println("Missing parent directory, Count: " + missingNameCount);
    System.out.println("Pending for delete (directory level): " + (total  / 1000000000) + " GB");
    System.out.println("Total entries in deleted directory table: " + deletedDirectoryToParentMap.size());
    System.out.println("Total entries of deleted directories calculated from file table: " + deletedDirectoriesSize.size());
  }

  private void dumpSnapshotDetails() throws IOException {
    Iterator<? extends Table.KeyValue<String, SnapshotInfo>> snapshots = snapshotInfoTable.iterator();
    while (snapshots.hasNext()) {
      Table.KeyValue<String, SnapshotInfo> data = snapshots.next();
      SnapshotInfo snapshot = data.getValue();
      String volume = snapshot.getVolumeName();
      String bucket = snapshot.getBucketName();
      String name = snapshot.getName();
      System.out.println("Snapshot: /" + volume + "/" + bucket + "/" + name);
    }
  }


  public void stop() throws Exception {
    store.close();
  }

  public static void main(String[] args) throws Exception{
    final String dbPath = "/Users/nvadivelu/Issues/Unionbank/ENGESC-29738/data/";
    /*
    if (args[0].endsWith("om.db") || args[0].endsWith("om.db/")) {
      dbPath = args[0].substring(0, args[0].lastIndexOf("om.db"));
    } else {
      dbPath = args[0];
    }
    */
      OMDBAnalyzer analyzer = new OMDBAnalyzer(dbPath);
      analyzer.calculateOBSBucketSize();
      analyzer.calculateFSOBucketSize();
      analyzer.printBucketSizes();
      analyzer.printDeletedDirectorySizes();
      analyzer.dumpSnapshotDetails();
      analyzer.stop();
  }

}
