# HBase Bulk Load Examples

As I was researching some HBase [bulk load](http://hbase.apache.org/book.html#arch.bulk.load) issues, I found it difficult to find usable code to test preparation of StoreFiles for the bulk load. So I wrote some!

## Use Cases
- Export from pre-existing HBase cluster
- Export from Parquet files

## Packaging

Maven is used for depedency management, so a simple `mvn package` generates your jar file.

## Running

Copy the jar to your HBase master and run the following command, depending on your use case.

The code is built to take an arbitrary table name to export.d

### HBase Export

```shell
HADOOP_CLASSPATH=$(hbase classpath) hadoop jar hbase-exporter-1.0-SNAPSHOT.jar BulkLoadDriver <SRC_TABLE> s3://<BUCKET>/<OUTPUT_PREFIX>/ hadoop
```

`HFileOutputFormat2.configureIncrementalLoad` in the `BulkLoadDriver` will use the `<SRC_TABLE>` in order to determine how many StoreFiles to generate.

### Parquet Source Files

Because we're bringing in an outside dependency (parquet), we need to build a shaded jar and include it in our classpath.

```shell
HADOOP_CLASSPATH=$(pwd)/hbase-exporter-1.0-SNAPSHOT.jar:$(hbase classpath) hadoop jar hbase-exporter-1.0-SNAPSHOT.jar \
  ParquetBulkLoadDriver \
  <DST_TABLE> \
  s3://<BUCKET>/<PARQUET_PREFIX>/ \
  s3://<BUCKET>/<OUTPUT_PREFIX>/
```

A table still needs to be provided when converting from Parquet so that `configureIncrementalLoad` can determine the right number of StoreFiles to create.

The columns to be read from the Parquet files is hard-coded, but could certainly be made dynamic with more time.

## Loading StoreFiles

Note that the current HBase documentation has a [completebulkload](http://hbase.apache.org/book.html#completebulkload) class path that isn't compatible with HBase 1.4.x.
`mapreduce` instead of `tool` needs to be used instead:

```shell
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles s3://<BUCKET>/<OUTPUT_PREFIX>/ <DST_TABLE>
```

## References

These sites were helpful when looking for example code:

http://hbase.apache.org/book.html#mapreduce.example
https://stackoverflow.com/questions/46246890/how-can-i-write-small-part-of-hbase-table-into-hfiles-without-loosing-any-column
http://milinda.pathirage.org/2016/12/11/hbase-bulk-load.html