package Moudle.HBase;

/**
 * Created by cluster on 2017/5/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;



public class RowFilterTest {


    private static HBaseConfig hBaseConfig = null;

    public void rowfilter(String tablename, String rowkeyvalue) {

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.31.63,192.168.31.61,192.168.31.62");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");

        try {
            HTable table = new HTable(conf, tablename);

            Filter filter1 = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes(rowkeyvalue)));
            // EQUAL =

            Scan s = new Scan();
            s.setFilter(filter1);
            ResultScanner rs = table.getScanner(s);

            for (Result r : rs) {
                System.out.println("rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列族:" + new String(keyValue.getFamily())
                            + " 列:" + new String(keyValue.getQualifier()) + ":"
                            + new String(keyValue.getValue()));
                }
            }

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

        }

    }



    public static void main(String[] args){
        RowFilterTest rft = new RowFilterTest();
        rft.rowfilter("user","id001");
    }

}
