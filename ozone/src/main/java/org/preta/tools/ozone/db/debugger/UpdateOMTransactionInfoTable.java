package org.preta.tools.ozone.db.debugger;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import java.io.IOException;

public class UpdateOMTransactionInfoTable {

  OzoneConfiguration conf;
  OmMetadataManagerImpl metadataStore;

  public UpdateOMTransactionInfoTable(String dbPath) throws IOException {
    this.conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbPath);
    this.metadataStore = new OmMetadataManagerImpl(conf, null);
    this.metadataStore.start(conf);
  }

  private void updateTransactionInfoTable(long term, long index) throws IOException {
    Table<String, TransactionInfo> transactionInfo = metadataStore.getTransactionInfoTable();
    System.out.println("TransactionInfo before update" + transactionInfo.get(OzoneConsts.TRANSACTION_INFO_KEY));
    TransactionInfo txnInfo = null; // TransactionInfo.valueOf(term, index);
    transactionInfo.put(OzoneConsts.TRANSACTION_INFO_KEY, txnInfo);
    System.out.println("TransactionInfo after update" + transactionInfo.get(OzoneConsts.TRANSACTION_INFO_KEY));
  }

  public void stop() throws Exception {
    metadataStore.stop();
  }

  public static void main(String[] args) throws Exception{
    if (args.length < 3) {
      System.err.println("Incorrect argument!");
      System.err.println("Usage:");
      System.err.println("UpdateOMTransactionInfoTable <path to om.db> <term> <index>");
      System.exit(2);
    }

    String dbPath;
    if (args[0].endsWith("om.db") || args[0].endsWith("om.db/")) {
      dbPath = args[0].substring(0, args[0].lastIndexOf("om.db"));
    } else {
      dbPath = args[0];
    }
    long term = Long.parseLong(args[1]);
    long index = Long.parseLong(args[2]);
    UpdateOMTransactionInfoTable updater = new UpdateOMTransactionInfoTable(dbPath);
    updater.updateTransactionInfoTable(term, index);
    updater.stop();
  }
}
