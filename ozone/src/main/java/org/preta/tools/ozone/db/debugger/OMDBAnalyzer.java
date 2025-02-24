package org.preta.tools.ozone.db.debugger;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
//import org.apache.ratis.server.protocol.TermIndex;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OMDBAnalyzer {

  OzoneConfiguration conf;
  OMMetadataManager metadataStore;
  Map<Long, Long> containers;
  Long totalSize = 0L;

  Set<String> files;

  public OMDBAnalyzer(String dbPath) throws IOException {
    this.conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbPath);
    this.metadataStore = new OmMetadataManagerImpl(conf, null);
    this.metadataStore.start(conf);

  }

  public void parseAllTables() throws Exception {
    Table<String, OmKeyInfo> keyTable = metadataStore.getKeyTable(BucketLayout.OBJECT_STORE);
    //parseTable(keyTable);
    Table<String, OmKeyInfo> fileTable = metadataStore.getKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    //parseTable(fileTable);
    Table<String, OmKeyInfo> openKeyTable = metadataStore.getOpenKeyTable(BucketLayout.OBJECT_STORE);
    //parseTable(openKeyTable);
    Table<String, OmKeyInfo> openFileTable = metadataStore.getOpenKeyTable(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    //parseTable(openFileTable);
    Table<String, TransactionInfo> transactionInfo = metadataStore.getTransactionInfoTable();
  }

    public void parseTable(Table<String, OmKeyInfo> table) throws Exception {
      Iterator<? extends Table.KeyValue<String, OmKeyInfo>> tableIter = table.iterator();
      while (tableIter.hasNext()) {
        Table.KeyValue<String, OmKeyInfo> data = tableIter.next();
        OmKeyInfo keyInfo = data.getValue();
        List<OmKeyLocationInfoGroup> groups = keyInfo.getKeyLocationVersions();
        for (OmKeyLocationInfoGroup group : groups) {
          List<OmKeyLocationInfo> locations = group.getLocationList();
            for (OmKeyLocationInfo location : locations) {
              containers.putIfAbsent(location.getContainerID(), 0L);
              containers.compute(location.getContainerID(), (k, v) -> v + 1);
            }
        }
      }
    }

    private void updateTransactionInfoTable() throws IOException {
      Table<String, TransactionInfo> transactionInfo = metadataStore.getTransactionInfoTable();
      System.out.println(transactionInfo.get(OzoneConsts.TRANSACTION_INFO_KEY));
      //TermIndex termIndex = TermIndex.valueOf(2, 2451);
      //TransactionInfo txnInfo = TransactionInfo.fromTermIndex(termIndex);
      //transactionInfo.put(OzoneConsts.TRANSACTION_INFO_KEY, txnInfo);
    }

    public void printMissingFiles() {
      for (String file : files) {
        System.out.println(file);
      }
    }

    public void printContainerList() {
      for (Map.Entry<Long, Long> entry : containers.entrySet()) {
        System.out.println(entry.getKey() + ", " + entry.getValue());
      }

        System.out.println("Total Size: " + totalSize);
    }

    public void dumpSnapshotInfo() throws Exception {
      System.out.println("=========Snapshot DB=============");
      Table<String, SnapshotInfo> snapshotInfoTable = metadataStore.getSnapshotInfoTable();
      Iterator<? extends Table.KeyValue<String, SnapshotInfo>>
          snapshotIter = snapshotInfoTable.iterator();
      while (snapshotIter.hasNext()) {
        Table.KeyValue<String, SnapshotInfo> data = snapshotIter.next();
        SnapshotInfo snapInfo = data.getValue();
        System.out.println(snapInfo);
      }
    }

    public void stop() throws Exception {
      metadataStore.stop();
    }


  public static void main(String[] args) throws Exception{
      OMDBAnalyzer analyzer = new OMDBAnalyzer(args[0]);
      analyzer.updateTransactionInfoTable();
      //analyzer.parseAllTables();
      //analyzer.printMissingFiles();
      analyzer.stop();
  }

}
