package Moudle.HBase.HBaseExample;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * 讲解了另外一种使用hbase客户端的例子，在这个例子里面table是一个轻量级的对象，
 * 在线程启动时创建退出时销毁，而table后面的connection从未关闭过，connection是重量级的对象，
 * 一直维持着和zookeeper的链接、异步操作和其他状态，我们可以从中学习到另外一种多线程操作hbase客户端的例子。
 */

/**
 * 本例用于展示在多线程中操作hbase客户端
 * 本例中table是一个轻量级的对象，在线程启动时创建退出时销毁，而table后面的connection从未关闭过
 * 本例中connection是重量级的对象，一直维持着和zookeeper的链接、异步操作和其他状态
 * 本例中模拟向hbase服务端提交500000次请求(其中30% 批量写，20%单条写，50%用于scans )
 *
 */
public class MultiThreadedClientExample extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(MultiThreadedClientExample.class);
    private static final int DEFAULT_NUM_OPERATIONS = 500000;

    //默认测试的是hbase的数据表test列簇d
    private static final byte[] FAMILY = Bytes.toBytes("basic");
    private static final byte[] QUAL = Bytes.toBytes("age");

    private final ExecutorService internalPool;//线程池
    private final int threads;//线程池大小

    public MultiThreadedClientExample() throws IOException {
        // Runtime.getRuntime().availableProcessors() 为当前机器CPU核数，这里取CPU核数* 4
        this.threads = Runtime.getRuntime().availableProcessors() * 4;

        // 这里调用google的guava-12.0.0.1.jar的ThreadFactoryBuilder，默认创建的是Executors.defaultThreadFactory()，创建的是后台线程工厂类，规范化了线程的名称
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("internal-pol-%d").build();
        //初始化线程池
        this.internalPool = Executors.newFixedThreadPool(threads, threadFactory);
    }

    @Override
    public int run(String[] args) throws Exception {
        //参数个数只能为2个，第一个为表名，第二个为操作的次数
        if (args.length < 1 || args.length > 2) {
            System.out.println("Usage: " + this.getClass().getName() + " tableName [num_operations]");
            return -1;
        }

        final TableName tableName = TableName.valueOf(args[0]);//如果传入了表名，就使用传入的hbase表名
        int numOperations = DEFAULT_NUM_OPERATIONS;
        if (args.length == 2) {
            numOperations = Integer.parseInt(args[1]);//如果传入了操作次数，就使用传入的操作次数
        }

        //Fork/Join框架是Java7提供了的一个用于并行执行任务的框架， 是一个把大任务分割成若干个小任务，最终汇总每个小任务结果后得到大任务结果的框架。
        //这里ForkJoinPool相继传入org.apache.hadoop.hbase.client.ConnectionManager.HConnectionImplementation、org.apache.hadoop.hbase.client.HTable、org.apache.hadoop.hbase.client.AsyncProcess使用
        ExecutorService service = new ForkJoinPool(threads * 2);

        //add by cpeixin 后添加的配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.31.63,192.168.31.61,192.168.31.62");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");


        // 为写操作创建一个单独的链接writeConnection
        final Connection writeConnection = ConnectionFactory.createConnection(getConf(), service);
        // 为读操作创建一个单独的链接readConnection
        final Connection readConnection = ConnectionFactory.createConnection(getConf(), service);

        // hbase 表tableName的region信息加载到cache
        // 这个操作在region个数超过250000个时不要操作
        warmUpConnectionCache(readConnection, tableName);
        warmUpConnectionCache(writeConnection, tableName);

        List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(numOperations);
        for (int i = 0; i < numOperations; i++) {
            //生成线程安全的随机浮点数r
            double r = ThreadLocalRandom.current().nextDouble();
            Future<Boolean> f;

            if (r < .30) {//30% 批量写
                f = internalPool.submit(new WriteExampleCallable(writeConnection, tableName));
            } else if (r < .50) {//20%单条写
                f = internalPool.submit(new SingleWriteExampleCallable(writeConnection, tableName));
            } else {//50%用于scans
                f = internalPool.submit(new ReadExampleCallable(writeConnection, tableName));
            }
            futures.add(f);
        }

        // 等待每个操作完成，如果没完成，等待10分钟
        for (Future<Boolean> f : futures) {
            f.get(10, TimeUnit.MINUTES);
        }

        // 关闭线程池internalPool和service
        internalPool.shutdownNow();
        service.shutdownNow();
        return 0;
    }

    // hbase 表tableName的region信息加载到cache
    // 这个操作在region个数超过250000个时不要操作
    private void warmUpConnectionCache(Connection connection, TableName tn) throws IOException {
        try (RegionLocator locator = connection.getRegionLocator(tn)) {
            LOG.info("Warmed up region location cache for " + tn + " got " + locator.getAllRegionLocations().size());
        }
    }

    /**
     * 30% 批量写任务
     */
    public static class WriteExampleCallable implements Callable<Boolean> {
        private final Connection connection;
        private final TableName tableName;

        public WriteExampleCallable(Connection connection, TableName tableName) {
            this.connection = connection;
            this.tableName = tableName;
        }

        @Override
        public Boolean call() throws Exception {
            // https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
            //这里利用jdk1.7里的新特性try(必须实现java.io.Closeable的对象){}catch (Exception e) {}
            //相当于调用了finally功能，调用(必须实现java.io.Closeable的对象)的close()方法，也即会调用table.close()
            try (Table t = connection.getTable(tableName)) {
                byte[] value = Bytes.toBytes(Double.toString(ThreadLocalRandom.current().nextDouble()));
                int rows = 30;

                // Array to put the batch
                ArrayList<Put> puts = new ArrayList<>(rows);
                for (int i = 0; i < 30; i++) {
                    byte[] rk = Bytes.toBytes(ThreadLocalRandom.current().nextLong());
                    Put p = new Put(rk);
                    p.addImmutable(FAMILY, QUAL, value);
                    puts.add(p);
                }

                // 批量提交到hbase服务端
                t.put(puts);
            }
            return true;
        }
    }

    /**
     * 20%单条写任务
     */
    public static class SingleWriteExampleCallable implements Callable<Boolean> {
        private final Connection connection;
        private final TableName tableName;

        public SingleWriteExampleCallable(Connection connection, TableName tableName) {
            this.connection = connection;
            this.tableName = tableName;
        }

        @Override
        public Boolean call() throws Exception {
            // https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
            //这里利用jdk1.7里的新特性try(必须实现java.io.Closeable的对象){}catch (Exception e) {}
            //相当于调用了finally功能，调用(必须实现java.io.Closeable的对象)的close()方法，也即会调用table.close()
            try (Table t = connection.getTable(tableName)) {
                byte[] value = Bytes.toBytes(Double.toString(ThreadLocalRandom.current().nextDouble()));
                byte[] rk = Bytes.toBytes(ThreadLocalRandom.current().nextLong());
                Put p = new Put(rk);
                p.addImmutable(FAMILY, QUAL, value);
                t.put(p);
            }
            return true;
        }
    }

    /**
     * 50%用于scans
     */
    public static class ReadExampleCallable implements Callable<Boolean> {
        private final Connection connection;
        private final TableName tableName;

        public ReadExampleCallable(Connection connection, TableName tableName) {
            this.connection = connection;
            this.tableName = tableName;
        }

        @Override
        public Boolean call() throws Exception {
            // total length in bytes of all read rows.
            int result = 0;

            // Number of rows the scan will read before being considered done.
            int toRead = 100;
            try (Table t = connection.getTable(tableName)) {
                //要朝找的rowkey的起始值
                byte[] rk = Bytes.toBytes(ThreadLocalRandom.current().nextLong());
                Scan s = new Scan(rk);

                //设置scan的filter为KeyOnlyFilter，意思是scan比较的时候只着重比较rowkey
                s.setFilter(new KeyOnlyFilter());

                //每次只取20条数据
                s.setCaching(20);

                //设置hbase不适用缓存，缓存是为了下次取这些数据更快，就把之前的数据放置到hbase服务端的blockcache
                s.setCacheBlocks(false);

                // https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
                //这里利用jdk1.7里的新特性try(必须实现java.io.Closeable的对象){}catch (Exception e) {}
                //相当于调用了finally功能，调用(必须实现java.io.Closeable的对象)的close()方法，也即会调用ResultScanner.close()
                try (ResultScanner rs = t.getScanner(s)) {
                    // 遍历hbase的行
                    for (Result r : rs) {
                        result += r.getRow().length;
                        toRead -= 1;

                        // 只取100条数据，达到100条就退出
                        if (toRead <= 0) {
                            break;
                        }
                    }
                }
            }
            return result > 0;
        }
    }

    public static void main(String[] args) throws Exception {
        //调用工具类ToolRunner执行实现了接口Tool的对象MultiThreadedClientExample的run方法，同时会把String[] args传入MultiThreadedClientExample的run方法
        ToolRunner.run(new MultiThreadedClientExample(), new String[]{"user"});
    }
}