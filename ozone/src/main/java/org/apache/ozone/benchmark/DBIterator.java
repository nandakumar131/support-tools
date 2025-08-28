package org.apache.ozone.benchmark;

import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;

public class DBIterator {

    OzoneConfiguration conf;
    OMMetadataManager metadataStore;

    public DBIterator(String dbPath) throws IOException {
      this.conf = new OzoneConfiguration();
      this.conf.set(OMConfigKeys.OZONE_OM_DB_DIRS, dbPath);
      this.metadataStore = new OmMetadataManagerImpl(conf, null);
      this.metadataStore.start(conf);
    }

    public void normalIterator() throws Exception {
      long totalCount = 0;
      final Table<String, OmKeyInfo> fileTable = metadataStore.getFileTable();
      final Iterator<? extends Table.KeyValue<String, OmKeyInfo>> tableIter = fileTable.iterator();
      while (tableIter.hasNext()) {
        final Table.KeyValue<String, OmKeyInfo> ignore = tableIter.next();
        totalCount++;
      }

      System.out.println("NormalIterator");
      System.out.println("Total count: " + totalCount);

    }

  public void serialStream() throws Exception {
    long totalCount = getFileTableStream().count();

    System.out.println("SerialStream");
    System.out.println("Total count: " + totalCount);

  }

  public void parallelStream() throws Exception {
    long totalCount = getFileTableStream().parallel().count();

    System.out.println("ParallelStream");
    System.out.println("Total count: " + totalCount);
  }

  public void stop() throws Exception {
    metadataStore.stop();
  }


  public Stream<Entry> getFileTableStream()
      throws IOException {
    Table<String, OmKeyInfo> fileTable = metadataStore.getFileTable();
    Iterator<? extends Table.KeyValue<String, OmKeyInfo>> tableIter = fileTable.iterator();
    Iterator<Entry> iter = new Iterator<Entry>() {

      @Override
      public boolean hasNext() {
        return tableIter.hasNext();
      }

      @Override
      public Entry next() {
        final Table.KeyValue<String, OmKeyInfo> data = tableIter.next();
        try {
          return new Entry(data.getKey(), data.getValue());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };

    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
        iter, Spliterator.ORDERED | Spliterator.NONNULL), false);
  }

  public static class Entry {
    private final String key;
    private final OmKeyInfo value;

    private Entry(String key, OmKeyInfo value) {
      this.key = key;
      this.value = value;
    }

    public String getKey() {
      return key;
    }

    public OmKeyInfo getValue() {
      return value;
    }
  }

    public static void main(String[] args) throws Exception{

      if (args.length < 1) {
        System.err.println("Incorrect argument!");
        System.err.println("Usage:");
        System.err.println("DBIterator <path to om.db>");
        System.exit(1);
      }
      final String dbPath;
      if (args[0].endsWith("om.db") || args[0].endsWith("om.db/")) {
        dbPath = args[0].substring(0, args[0].lastIndexOf("om.db"));
      } else {
        dbPath = args[0];
      }
      DBIterator iterator = new DBIterator(dbPath);
      iterator.normalIterator();
      iterator.serialStream();
      iterator.parallelStream();
      iterator.stop();
    }
}
