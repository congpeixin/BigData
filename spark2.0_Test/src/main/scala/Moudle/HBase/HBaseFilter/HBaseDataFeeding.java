package Moudle.HBase.HBaseFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 写入数据代码
 */
public class HBaseDataFeeding {
    private final static byte[] ROW1 = Bytes.toBytes("row1");
    private final static byte[] ROW2 = Bytes.toBytes("row2");
    private final static byte[] COLFAM1 = Bytes.toBytes("colfam1");
    private final static byte[] COLFAM2 = Bytes.toBytes("colfam2");
    private final static byte[] QUAL1 = Bytes.toBytes("qual1");
    private final static byte[] QUAL2 = Bytes.toBytes("qual2");


    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "testtable");
        table.setAutoFlushTo(false);
        Put put_row1 = new Put(ROW1);
        put_row1.add(COLFAM1, QUAL1, Bytes.toBytes("ROW1_QUAL1_VAL"));
        put_row1.add(COLFAM1, QUAL2, Bytes.toBytes("ROW1_QUAL2_VAL"));

        Put put_row2 = new Put(ROW2);
        put_row2.add(COLFAM1, QUAL1, Bytes.toBytes("ROW2_QUAL1_VAL"));
        put_row2.add(COLFAM1, QUAL2, Bytes.toBytes("ROW2_QUAL2_VAL"));

        try{
            table.put(put_row1);
            table.put(put_row2);
        }finally{
            table.close();
        }
    }

}
