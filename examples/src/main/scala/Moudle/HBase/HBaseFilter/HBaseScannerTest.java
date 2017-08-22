package Moudle.HBase.HBaseFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 过滤器练习代码
 */
public class HBaseScannerTest {

    public static void main(String[] args) throws IOException, IllegalAccessException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "testtable");
        table.setAutoFlushTo(false);

        Scan scan1 = new Scan();
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(
                Bytes.toBytes("colfam1"),
                Bytes.toBytes("qual2"),
                CompareFilter.CompareOp.NOT_EQUAL,
                new SubstringComparator("BOGUS"));
        scvf.setFilterIfMissing(false);
        scvf.setLatestVersionOnly(true); // OK
        Filter ccf = new ColumnCountGetFilter(2); // OK 如果突然发现一行中的列数超过设定的最大值时，整个扫描操作会停止
        Filter vf = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("ROW2_QUAL1")); // OK 筛选某个（值的条件满足的）特定的单元格
        Filter cpf = new ColumnPrefixFilter(Bytes.toBytes("qual2")); // OK 筛选出前缀匹配的列
        Filter fkof = new FirstKeyOnlyFilter(); // OK 筛选出第一个每个第一个单元格
        Filter isf = new InclusiveStopFilter(Bytes.toBytes("row1")); // OK 包含了扫描的上限在结果之内
        Filter rrf = new RandomRowFilter((float) 0.8); // OK 随机选出一部分的行
        Filter kof = new KeyOnlyFilter(); // OK 返回所有的行，但值全是空
        Filter pf = new PrefixFilter(Bytes.toBytes("row")); // OK  筛选匹配行键的前缀成功的行
        Filter rf = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("row1"))); // OK 筛选出匹配的所有的行
        Filter wmf = new WhileMatchFilter(rf); // OK 类似于Python itertools中的takewhile
        Filter skf = new SkipFilter(vf); // OK 发现某一行中的一列需要过滤时，整个行就会被过滤掉

        List<Filter> filters = new ArrayList<Filter>();
        filters.add(rf);
        filters.add(vf);
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters); // OK 综合使用多个过滤器， AND 和 OR 两种关系

        scan1.
                setStartRow(Bytes.toBytes("row1")).
                setStopRow(Bytes.toBytes("row3")).
                setFilter(scvf);
        ResultScanner scanner1 = table.getScanner(scan1);

        for(Result res : scanner1){
            for(Cell cell : res.rawCells()){
                System.out.println("KV: " + cell + ", Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println("------------------------------------------------------------");
        }

        scanner1.close();
        table.close();
    }

}
