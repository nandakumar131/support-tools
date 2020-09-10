# Ozone Support Tools

### MetaGen
Tool to generate Ozone Metadata

````bash
Usage:
bin/ozone-tools metagen [-hV] [COMMAND]
Tool to generate Ozone metadata.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  om  Generate OzoneManager metadata.
````

#### OM
Generates Ozone Manager Metadata

````bash
Usage:
bin/ozone-tools metagen om [-hV] [-b=<bucket>] [-c=<count>] [-p=<path>]
                           [-u=<user>] [-v=<volume>]
Generate OzoneManager metadata.
  -b, --bucket=<bucket>   Ozone Bucket to create.
  -c, --count=<count>     Number of keys to create.
  -h, --help              Show this help message and exit.
  -p, --dbPath=<path>     Path to generate OzoneManager db.
  -u, --user=<user>       Ozone User to create.
  -v, --volume=<volume>   Ozone Volume to create.
  -V, --version           Print version information and exit.
````

### Benchmark
Tool to benchmark Ozone

````bash
Usage:
bin/ozone-tools benchmark [-hV] [COMMAND]
Tool to benchmark Ozone services.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  ozone    Tool to benchmark Ozone.
  om       Tool to benchmark Ozone Manager.
  rocksdb  Tool to benchmark RocksDB.
````

#### Ozone
Benchmark Ozone throughput

````bash
Usage:
bin/ozone-tools benchmark ozone [-hV] [COMMAND]
Tool to benchmark Ozone.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  write  Benchmark Ozone Write Performance.
````

##### Write
Benchmark Ozone write throughput

````bash
Usage:
bin/ozone-tools benchmark ozone write [-hV] [-b=<bucket>] -d=<runtime>
       [-p=<keyNamePrefix>] [-s=<dataSize>] [-u=<user>] [-v=<volume>]
       [-w=<writerThreads>]
Benchmark Ozone Write Performance.
  -b, --bucket=<bucket>      Ozone Bucket.
  -d, --duration=<runtime>   Runtime. Can be specified in seconds, minutes or
                               hours using the s, m or h suffixes respectively.
                               Default unit is seconds.
  -h, --help                 Show this help message and exit.
  -p, --keyPrefix=<keyNamePrefix>
                             Key Prefix.
  -s, --dataSize=<dataSize>  Data size to write.
  -u, --user=<user>          User Name.
  -v, --volume=<volume>      Ozone Volume.
  -V, --version              Print version information and exit.
  -w, --numWriteThreads=<writerThreads>
                             Number of writer threads.
````

#### OM
Benchmark OzoneManager throughput

````bash
Usage:
bin/ozone-tools benchmark om [-hV] [COMMAND]
Tool to benchmark Ozone Manager.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  read-write  Benchmark OzoneManager Read/Write.
  read        Benchmark OzoneManager Read.
  write       Benchmark OzoneManager Write.
````

##### Read Write
Benchmark OzoneManager read-write throughput

````bash
Usage:
bin/ozone-tools benchmark om read-write [-hV] [-b=<bucket>] -d=<runtime>
       [-p=<keyNamePrefix>] [-r=<readerThreads>] [-u=<user>] [-v=<volume>]
       [-w=<writerThreads>]
Benchmark OzoneManager Read/Write.
  -b, --bucket=<bucket>      Ozone Bucket.
  -d, --duration=<runtime>   Runtime. Can be specified in seconds, minutes or
                               hours using the s, m or h suffixes respectively.
                               Default unit is seconds.
  -h, --help                 Show this help message and exit.
  -p, --keyPrefix=<keyNamePrefix>
                             Key Prefix.
  -r, --numReaderThreads=<readerThreads>
                             Number of reader threads.
  -u, --user=<user>          User Name.
  -v, --volume=<volume>      Ozone Volume.
  -V, --version              Print version information and exit.
  -w, --numWriteThreads=<writerThreads>
                             Number of writer threads.
  ````
  
##### Read
Benchmark OzoneManager read throughput

````bash
Usage:
bin/ozone-tools benchmark om read [-hV] [-b=<bucket>] -d=<runtime>
                                     [-p=<keyNamePrefix>] [-r=<readerThreads>]
                                     [-v=<volume>]
Benchmark OzoneManager Read.
  -b, --bucket=<bucket>      Ozone Bucket.
  -d, --duration=<runtime>   Runtime. Can be specified in seconds, minutes or
                               hours using the s, m or h suffixes respectively.
                               Default unit is seconds.
  -h, --help                 Show this help message and exit.
  -p, --keyPrefix=<keyNamePrefix>
                             Key Prefix.
  -r, --numReaderThreads=<readerThreads>
                             Number of reader threads.
  -v, --volume=<volume>      Ozone Volume.
  -V, --version              Print version information and exit.
````

##### Write
Benchmark OzoneManager write throughput

````bash
Usage:
bin/ozone-tools benchmark om write [-hV] [-b=<bucket>] -d=<runtime>
                                      [-p=<keyNamePrefix>] [-u=<user>]
                                      [-v=<volume>] [-w=<writerThreads>]
Benchmark OzoneManager Write.
  -b, --bucket=<bucket>      Ozone Bucket.
  -d, --duration=<runtime>   Runtime. Can be specified in seconds, minutes or
                               hours using the s, m or h suffixes respectively.
                               Default unit is seconds.
  -h, --help                 Show this help message and exit.
  -p, --keyPrefix=<keyNamePrefix>
                             Key Prefix.
  -u, --user=<user>          User Name.
  -v, --volume=<volume>      Ozone Volume.
  -V, --version              Print version information and exit.
  -w, --numWriteThreads=<writerThreads>
                             Number of writer threads.
````

#### Rocksdb
Benchmark Rocksdb throughput

````bash
Usage:
bin/ozone-tools benchmark rocksdb [-hV] [COMMAND]
Tool to benchmark RocksDB.
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.
Commands:
  write  Benchmark RocksDB Write.
````

##### Write
Benchmark Rocksdb write throughput

````bash
Usage:
bin/ozone-tools benchmark rocksdb write [-fhsV] [-b=<rocksDbBatchSize>]
       -d=<runtime> [-k=<keyNamePrefix>] [-n=<numBlocks>] [-p=<path>]
       [-w=<writerThreads>]
Benchmark RocksDB Write.
  -b, --rocksDbBatchSize=<rocksDbBatchSize>
                             Key Prefix.
  -d, --duration=<runtime>   Runtime. Can be specified in seconds, minutes or
                               hours using the s, m or h suffixes respectively.
                               Default unit is seconds.
  -f, --flush                Flush RocksDB WAL.
  -h, --help                 Show this help message and exit.
  -k, --keyPrefix=<keyNamePrefix>
                             Key Prefix.
  -n, --numBlocks=<numBlocks>
                             Key Prefix.
  -p, --path=<path>          DB Path.
  -s, --sync                 Sync RocksDB WAL, applicable only if flush is
                               enabled.
  -V, --version              Print version information and exit.
  -w, --numWriteThreads=<writerThreads>
                             Number of writer threads.
````
