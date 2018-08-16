import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

public class ParquetBulkLoadDriver {

    public static void main(String[] args) throws Exception {
        System.out.println(ParquetBulkLoadDriver.class + ".main: " + args[0]);
        submitBulkLoadJob(args[0], args[1], args[2]);
    }

    public static void submitBulkLoadJob(String hbaseTable, String sourceLocation, String outputLocation) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.fs.tmp.dir", "/tmp");

        Job job = Job.getInstance(conf, "HBase Parquet Loader: " + hbaseTable);
        job.setJarByClass(ParquetBulkLoadDriver.class);
        job.setMapperClass(ParquetMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setInputFormatClass(ExampleInputFormat.class);

        // Configure variables for job
        Path tmpPath = new Path(outputLocation);
        Connection hbCon = ConnectionFactory.createConnection(conf);
        Table hTable = hbCon.getTable(TableName.valueOf(hbaseTable));
        RegionLocator regionLocator = hbCon.getRegionLocator(TableName.valueOf(hbaseTable));
        Admin admin = hbCon.getAdmin();

        try {
            HFileOutputFormat2.configureIncrementalLoad(job, hTable, regionLocator);
            FileInputFormat.addInputPath(job, new Path(sourceLocation));
            HFileOutputFormat2.setOutputPath(job, tmpPath);
            job.waitForCompletion(true);
        } finally {
            hTable.close();
            regionLocator.close();
            admin.close();
            hbCon.close();
        }
    }
}
