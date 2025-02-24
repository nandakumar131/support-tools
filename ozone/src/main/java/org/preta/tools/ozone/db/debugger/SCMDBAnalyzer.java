package org.preta.tools.ozone.db.debugger;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.metadata.SCMMetadataStoreImpl;
import org.apache.hadoop.hdds.utils.db.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SCMDBAnalyzer {

  OzoneConfiguration conf;
  SCMMetadataStoreImpl metadataStore;
  Map<Long, Long> containers;

  public SCMDBAnalyzer(String dbPath) throws IOException {
    this.conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, dbPath);
    this.metadataStore = new SCMMetadataStoreImpl(conf);
    this.containers = new HashMap<>();
  }


 public void dumpContainerDB () throws Exception {
   System.out.println("=========Container DB=============");
    Set<String> owner = new HashSet<>();
    Table<ContainerID, ContainerInfo> containerTable = metadataStore.getContainerTable();
    Iterator<? extends Table.KeyValue<ContainerID, ContainerInfo>> containerIter = containerTable.iterator();
    while (containerIter.hasNext()) {
      Table.KeyValue<ContainerID, ContainerInfo> data = containerIter.next();
      ContainerInfo containerInfo = data.getValue();
      owner.add(containerInfo.getOwner());
      System.out.println(containerInfo);
    }
    containerTable.close();
   System.out.println("============================");
    System.out.println("Owner List");
   for (String o : owner) {
      System.out.println("Owner: " + o);
    }
    System.out.println("============================");

 }

  public void run() throws Exception {

    System.out.println("=========Containers from Delete Txn Table=============");


    long totalTxnCount = 0;
    long totalBlockCount = 0;
    long negativeTxnCount = 0;
    long negativeBlockCount = 0;

    Table<Long, DeletedBlocksTransaction> deleteTxTable = metadataStore.getDeletedBlocksTXTable();
    Iterator<? extends Table.KeyValue<Long, DeletedBlocksTransaction>>  deleteTxn = deleteTxTable.iterator();
    while (deleteTxn.hasNext()) {
      Table.KeyValue<Long, DeletedBlocksTransaction> data = deleteTxn.next();
      DeletedBlocksTransaction tx = data.getValue();
      totalTxnCount++;
      totalBlockCount += tx.getLocalIDCount();
      containers.putIfAbsent(tx.getContainerID(), 0L);
      containers.compute(tx.getContainerID(), (k, v) -> v + tx.getLocalIDCount());

      if (tx.getCount() < 0) {
        negativeTxnCount++;
        negativeBlockCount += tx.getLocalIDCount();
      }

    }

    deleteTxTable.close();

    for (Long id : containers.keySet()) {
      System.out.println("Container ID: " + id + ", Block Count: " + containers.get(id));
    }

    System.out.println("===========================================");
    System.out.println("Total delete Txn: " + totalTxnCount);
    System.out.println("Total number of blocks: " + totalBlockCount);
    System.out.println("Total delete Txn with negative retry count: " + negativeTxnCount);
    System.out.println("Total number of blocks with negative retry count: " + negativeBlockCount);
    System.out.println("===========================================");

  }


  public static void main(String[] args) throws Exception {
    SCMDBAnalyzer scmdbAnalyzer = new SCMDBAnalyzer(args[0]);
    scmdbAnalyzer.dumpContainerDB();
    //scmdbAnalyzer.run();

  }
}
