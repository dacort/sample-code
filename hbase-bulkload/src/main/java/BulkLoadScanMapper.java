import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;

import java.io.IOException;

public class BulkLoadScanMapper extends TableMapper<ImmutableBytesWritable, Put> {

    public void map(ImmutableBytesWritable row, Result value, Context context)
            throws InterruptedException, IOException
    {
        context.write(row, resultToPut(row,value));
    }

    private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
        Put put = new Put(key.get());
        for (org.apache.hadoop.hbase.KeyValue kv : result.raw()) {
            put.add(kv);
        }
        return put;
    }
}
