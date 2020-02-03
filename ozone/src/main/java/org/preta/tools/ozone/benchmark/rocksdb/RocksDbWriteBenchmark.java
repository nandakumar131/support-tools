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

package org.preta.tools.ozone.benchmark.rocksdb;

import org.apache.hadoop.conf.StorageUnit;
import org.preta.tools.ozone.ReadableTimestampConverter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import picocli.CommandLine;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.rocksdb.TickerType.COMPACT_READ_BYTES;
import static org.rocksdb.TickerType.COMPACT_WRITE_BYTES;
import static org.rocksdb.TickerType.STALL_MICROS;


@CommandLine.Command(name = "write",
    description = "Benchmark RocksDB Write.",
    mixinStandardHelpOptions = true)
public class RocksDbWriteBenchmark implements Runnable {

  @CommandLine.Option(names = {"-d", "--duration"},
      required = true,
      converter = ReadableTimestampConverter.class,
      description = "Runtime. Can be specified in seconds, minutes or hours " +
          "using the s, m or h suffixes respectively. Default unit is seconds.")
  private long runtime;

  @CommandLine.Option(names = {"-p", "--path"},
      description = "DB Path.")
  private String path;

  @CommandLine.Option(names = {"-w", "--numWriteThreads"},
      description = "Number of writer threads.")
  private int writerThreads;

  @CommandLine.Option(names = {"-k", "--keyPrefix"},
      description = "Key Prefix.")
  private String keyNamePrefix;

  @CommandLine.Option(names = {"-b", "--numBlocks"},
      description = "Key Prefix.")
  private int numBlocks;

  private static final String OPEN_KEY_TABLE = "OpenKeyTable";
  private static final String KEY_TABLE = "KeyTable";

  private static AtomicLong keyCount = new AtomicLong(0);

  private RocksDB db;
  private long endTimeInNs;
  private Map<String, ColumnFamilyHandle> handlers = new HashMap<>();
  private Statistics statistics;

  private Map<String, byte[]> openKeys = new HashMap<>();

  private boolean useInMemoryOpenKeyStore;

  public RocksDbWriteBenchmark() {
    this.path = "/tmp/test.db";
    this.keyNamePrefix = "/instagram/images/";
    this.writerThreads = 10;
    this.numBlocks = 1;
  }

  public void run() {
    final long startTime = System.nanoTime();
    endTimeInNs  = startTime + (runtime * 1000000000L);
    final DBOptions dbOptions = getDBOptions();
    final List<ColumnFamilyDescriptor> columnFamilyDescriptors = getColumnFamilies();
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    try {
      db = RocksDB.open(dbOptions, path, columnFamilyDescriptors, columnFamilyHandles);
      db.resetStats();
      for (ColumnFamilyHandle handle : columnFamilyHandles) {
        handlers.put(new String(handle.getName()), handle);
      }

      final ExecutorService writeExecutor = Executors.newFixedThreadPool(writerThreads);
      for (int i = 0; i < this.writerThreads; i++) {
        writeExecutor.submit(this::writeKeys);
      }
      writeExecutor.shutdown();
      writeExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.MINUTES);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      System.out.println("Time (sec) : " + ((System.nanoTime() - startTime)/1000000000L));
      System.out.println("Number of keys: " + keyCount.get());
      System.out.println("STALL_MICROS : " + statistics.getTickerCount(STALL_MICROS));
      System.out.println("COMPACT_READ_BYTES: " + statistics.getTickerCount(COMPACT_READ_BYTES));
      System.out.println("COMPACT_WRITE_BYTES: " + statistics.getTickerCount(COMPACT_WRITE_BYTES));
      System.out.println("statistics: " + statistics.toString());
      if (db != null) {
        db.close();
      }
    }
  }

  private DBOptions getDBOptions() {
    final int maxBackgroundCompactions = 4;
    final int maxBackgroundFlushes = 2;
    final long bytesPerSync = toLong(StorageUnit.MB.toBytes(1.00));
    final boolean createIfMissing = true;
    final boolean createMissingColumnFamilies = true;
    final DBOptions options = new DBOptions();
    statistics = new Statistics();
    options.setStatistics(statistics)
        .setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
        .setMaxBackgroundCompactions(maxBackgroundCompactions)
        .setMaxBackgroundFlushes(maxBackgroundFlushes)
        .setBytesPerSync(bytesPerSync)
        .setCreateIfMissing(createIfMissing)
        .setCreateMissingColumnFamilies(createMissingColumnFamilies);
    return options;
  }

  private List<ColumnFamilyDescriptor> getColumnFamilies() {
    final List<ColumnFamilyDescriptor> columnFamilies = new ArrayList<>();
    columnFamilies.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, getColumnFamilyOptions()));
    columnFamilies.add(new ColumnFamilyDescriptor(OPEN_KEY_TABLE.getBytes(StandardCharsets.UTF_8), getColumnFamilyOptions()));
    columnFamilies.add(new ColumnFamilyDescriptor(KEY_TABLE.getBytes(StandardCharsets.UTF_8), getColumnFamilyOptions()));
    return columnFamilies;
  }

  private ColumnFamilyOptions getColumnFamilyOptions() {

    // Set BlockCacheSize to 256 MB. This should not be an issue for HADOOP.
    final long blockCacheSize = toLong(StorageUnit.MB.toBytes(256.00));

    // Set the Default block size to 16KB
    final long blockSize = toLong(StorageUnit.KB.toBytes(16));

    // Write Buffer Size -- set to 128 MB
    final long writeBufferSize = toLong(StorageUnit.MB.toBytes(128));

    return new ColumnFamilyOptions()
        .setLevelCompactionDynamicLevelBytes(true)
        .setWriteBufferSize(writeBufferSize)
        .setTableFormatConfig(
            new BlockBasedTableConfig()
                .setBlockCacheSize(blockCacheSize)
                .setBlockSize(blockSize)
                .setCacheIndexAndFilterBlocks(true)
                .setPinL0FilterAndIndexBlocksInCache(true)
                .setFilter(new BloomFilter()));
  }

  private void writeKeys() {
    try {
      final Random ran = new Random();
      byte[] valuePrefix = new byte[500];
      ran.nextBytes(valuePrefix);
      while (System.nanoTime() < endTimeInNs) {
        byte[] val = new byte[100];
        ran.nextBytes(val);
        final ByteBuffer buff = ByteBuffer.wrap(new byte[valuePrefix.length + val.length]);
        buff.put(valuePrefix)
            .put(val);
        final String keyString = keyNamePrefix + UUID.randomUUID().toString();
        final byte[] key = keyString.getBytes(StandardCharsets.UTF_8);

        // Check if key doesn't exist in KeyTable
        if (db.get(handlers.get(KEY_TABLE), key) != null) {
          System.err.println("Key already exist! key: " + keyString);
          continue;
        }

        // Write it to OpenKeyTable
        //addToOpenKeyTable(key, buff.array());
        addToOpenKeyMap(keyString, buff.array());

        // Add block
        for (int i = 1; i < numBlocks; i++) {
          //final byte[] data = db.get(handlers.get(OPEN_KEY_TABLE), key);
          final byte[] data = openKeys.get(keyString);
          final byte[] block = new byte[100];
          ran.nextBytes(block);
          final ByteBuffer newBuff = ByteBuffer.wrap(new byte[data.length + block.length]);
          newBuff.put(data)
              .put(block);
          //addToOpenKeyTable(key, newBuff.array());
          addToOpenKeyMap(keyString, newBuff.array());
        }

        // Move from OpenKeyTable to KeyTable
        //final byte[] data = db.get(handlers.get(OPEN_KEY_TABLE), key);
        db.put(handlers.get(KEY_TABLE), key, openKeys.get(keyString));
        //db.delete(handlers.get(OPEN_KEY_TABLE), key);
        openKeys.remove(keyString);

        keyCount.incrementAndGet();
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void addToOpenKeyTable(byte[] key, byte[] value) throws RocksDBException {
    db.put(handlers.get(OPEN_KEY_TABLE), key, value);
  }

  private void addToOpenKeyMap(String key, byte[] value) {
    openKeys.put(key, value);
  }


  private static long toLong(double value) {
    BigDecimal temp = BigDecimal.valueOf(value);
    return temp.longValue();
  }

}
