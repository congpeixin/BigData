package Moudle.HBase.HBaseFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by cluster on 2017/5/19.
 */
public class HBaseFilterTest {

    private static Configuration conf;
    private static Connection con;
    private static final String tableName = "user";
    // 初始化连接
    static {
        conf = HBaseConfiguration.create(); // 获得配制文件对象
        conf.set("hbase.zookeeper.quorum","192.168.31.63,192.168.31.61,192.168.31.62");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        try {
            con = ConnectionFactory.createConnection(conf);// 获得连接对象
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // 获得连接
    public static Connection getCon() {
        if (con == null || con.isClosed()) {
            try {
                con = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return con;
    }
    // 关闭连接
    public static void close() {
        if (con != null) {
            try {
                con.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    TableName tn = TableName.valueOf(tableName);

    //过滤器，只返回行健
    public void KeyOnlyFilter() throws IOException {
        //创建set集合，存放key值
        Set<String> keySet = new HashSet<String>();

        Table table = getCon().getTable(TableName.valueOf(tableName));
        Filter filter = new KeyOnlyFilter(false); // OK 返回所有的行，但值全是空

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner  = table.getScanner(scan);
        for(Result res:scanner){
            for(KeyValue kv: res.raw()){
                keySet.add(Bytes.toString(kv.getRow()));
//                System.out.println("KV: "+Bytes.toString(kv.getRow()));
            }

        }


        for (String key: keySet){
            System.out.println(key);
        }


        scanner.close();
    }



    //筛选出具有特定前缀的行键的数据
    public void PrefixFilter() throws IOException {
        Table table = getCon().getTable(TableName.valueOf(tableName));
        Filter filter = new PrefixFilter(Bytes.toBytes("id1")); // OK  筛选匹配行键的前缀成功的行
        //此次查询，rowkey中有id001,id002,id003,id100。  筛选条件设置成id1，则可筛选出 rowkey为id100的数据
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner  = table.getScanner(scan);
        for(Result res:scanner){
            for(KeyValue kv: res.raw()){
                System.out.println("KV: "+kv + ",value: "+Bytes.toString(kv.getValue()));
            }
        }
        scanner.close();

//        Get get = new Get(Bytes.toBytes("wuchong"));
//        get.setFilter(filter);
//        Result result = table.get(get);
//        System.out.println("Result of get(): " + result);
//        for(KeyValue kv:result.raw()){
//            System.out.println("KV: "+kv + ",value:"+ Bytes.toString(kv.getValue()));
//        }
    }



    /**
     * 单列值过滤器
     * @throws IOException
     */
    public void singleColumnValueFilter() throws IOException {
        Table table = getCon().getTable(TableName.valueOf(tableName));
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("basic"),
                Bytes.toBytes("name"), CompareFilter.CompareOp.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("dpark")));
        filter.setFilterIfMissing(true); //所有不包含参考列的行都可以被过滤掉，默认这一行包含在结果中

        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner scanner  = table.getScanner(scan);
        for(Result res:scanner){
            for(KeyValue kv: res.raw()){
                System.out.println("KV: "+kv + ",value: "+Bytes.toString(kv.getValue()));
            }
        }
        scanner.close();

        Get get = new Get(Bytes.toBytes("wuchong"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println("Result of get(): " + result);
        for(KeyValue kv:result.raw()){
            System.out.println("KV: "+kv + ",value:"+ Bytes.toString(kv.getValue()));
        }
    }




    public static void main(String[] args) throws IOException {
        HBaseFilterTest hft = new HBaseFilterTest();
//        hft.PrefixFilter();
//        hft.singleColumnValueFilter();
        hft.KeyOnlyFilter();
    }
}
