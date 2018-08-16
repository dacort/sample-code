import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

public class BulkLoadDriver {

    public static void main(String[] args) throws Exception {
        System.out.println("In main..." + args[0]);
        submitBulkLoadJob(args[0], args[1], args[2]);
    }

    public static void submitBulkLoadJob(String hbaseTable, String output, String hadoopUser) throws Exception {
        System.setProperty("HADOOP_USER_NAME", hadoopUser);
        Configuration conf = HBaseConfiguration.create();;
        conf.set("hbase.fs.tmp.dir", "/tmp");

        Job job = Job.getInstance(conf, "HBase Bulk Data Prep: " + hbaseTable);
        job.setJarByClass(BulkLoadDriver.class);

        // Try using a table scanner
        Scan scan = new Scan();

        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setMaxResultSize(20000);
        scan.setMaxResultsPerColumnFamily(10000);
        scan.setFilter(new PageFilter(500));
        TableMapReduceUtil.initTableMapperJob(
                hbaseTable, // tableName
                scan,
                BulkLoadScanMapper.class, // mapper class
                ImmutableBytesWritable.class, // mapper output key
                Put.class, // mapper output value
                job
        );

        System.out.println("Setting output path: " + output);
        Path tmpPath = new Path(output);
        Connection hbCon = ConnectionFactory.createConnection(conf);
        Table hTable = hbCon.getTable(TableName.valueOf(hbaseTable));
        RegionLocator regionLocator = hbCon.getRegionLocator(TableName.valueOf(hbaseTable));
        Admin admin = hbCon.getAdmin();

        try {
            HFileOutputFormat2.setOutputPath(job, tmpPath);
            HFileOutputFormat2.configureIncrementalLoad(job, hTable, regionLocator);
            job.waitForCompletion(true);
        } finally {
            hTable.close();
            regionLocator.close();
            admin.close();
            hbCon.close();
        }
    }
}