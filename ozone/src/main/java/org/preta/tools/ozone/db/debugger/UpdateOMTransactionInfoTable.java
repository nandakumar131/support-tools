package org.preta.tools.ozone.db.debugger;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.ratis.server.protocol.TermIndex;
import java.io.IOException;

public class UpdateOMTransactionInfoTable {

  OzoneConfiguration conf;
  OMMetadataManager metadataStore;

  public UpdateOMTransactionInfoTable(String dbPath) throws IOException {
    this.conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbPath);
    this.metadataStore = new OmMetadataManagerImpl(conf, null);
    this.metadataStore.start(conf);
  }

  private void updateTransactionInfoTable(int term, int index) throws IOException {
    Table<String, TransactionInfo> transactionInfo = metadataStore.getTransactionInfoTable();
    System.out.println("TransactionInfo before update" + transactionInfo.get(OzoneConsts.TRANSACTION_INFO_KEY));
    TermIndex termIndex = TermIndex.valueOf(term, index);
    TransactionInfo txnInfo = TransactionInfo.fromTermIndex(termIndex);
    transactionInfo.put(OzoneConsts.TRANSACTION_INFO_KEY, txnInfo);
    System.out.println("TransactionInfo after update" + transactionInfo.get(OzoneConsts.TRANSACTION_INFO_KEY));
  }



  public void stop() throws Exception {
    metadataStore.stop();
  }


  public static void main(String[] args) throws Exception{
    if (args.length < 4) {
      System.err.println("Incorrect argument!");
      System.err.println("Usage:");
      System.err.println("UpdateOMTransactionInfoTable <path to om.db> <term> <index>");
      System.exit(2);
    }

    String dbPath;
    if (args[1].endsWith("om.db") || args[0].endsWith("om.db/")) {
      dbPath = args[1].substring(0, args[0].lastIndexOf("om.db"));
    } else {
      dbPath = args[1];
    }
    int term = Integer.parseInt(args[2]);
    int index = Integer.parseInt(args[3]);
    UpdateOMTransactionInfoTable updater = new UpdateOMTransactionInfoTable(dbPath);
    updater.updateTransactionInfoTable(term, index);
    updater.stop();
  }
}
