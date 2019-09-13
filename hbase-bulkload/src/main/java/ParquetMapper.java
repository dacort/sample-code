import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.parquet.example.data.Group;

import java.io.IOException;

// If we want to change this to an inner class, need to make it static
//https://stackoverflow.com/questions/11446635/no-such-method-exception-hadoop-init
public class ParquetMapper extends Mapper<LongWritable, Group, ImmutableBytesWritable, Put> {

    public void map(LongWritable key, Group value, Context context) throws InterruptedException, IOException {
        // Ensure we actually have data - e.g. empty Parquet file
        if (value.getFieldRepetitionCount("name") == 0) {
            return;
        }
        String outKey = value.getString("name", 0);

        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(outKey.getBytes());
        Put put = new Put(Bytes.toBytes(outKey));   // ROWKEY

        // Fields that we're importing are currently hard-coded
        put.addColumn("f1".getBytes(), "value".getBytes(), value.getString("value", 0).getBytes());
        put.addColumn("f1".getBytes(), "type".getBytes(), value.getString("type", 0).getBytes());

        context.write(rowKey, put);
    }
}


