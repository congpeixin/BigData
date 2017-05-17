package Moudle.HBase.HBaseExample;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * An example of using the {@link BufferedMutator} interface.
 */
public class BufferedMutatorExample extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(BufferedMutatorExample.class);

    private static final int POOL_SIZE = 10;// 线程池大小
    private static final int TASK_COUNT = 100;// 任务数
    private static final TableName TABLE = TableName.valueOf("foo");// hbase数据表foo
    private static final byte[] FAMILY = Bytes.toBytes("f");// hbase数据表foo的列簇f

    /**
     * 重写Tool.run(String [] args)方法，传入的是main函数的参数String[] args
     */
    @Override
    public int run(String[] args) throws InterruptedException, ExecutionException, TimeoutException {

        /** 一个异步回调监听器，在hbase write失败的时候触发. */
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    LOG.info("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        /**
         * BufferedMutator的构造参数对象BufferedMutatorParams. 
         * BufferedMutatorParams参数如下：
         *   			TableName tableName
         *   			long writeBufferSize
         *   			int maxKeyValueSize
         *  			 ExecutorService pool
         *  			 BufferedMutator.ExceptionListener listener
         *  这里只设置了属性tableName和listener
         * */
        BufferedMutatorParams params = new BufferedMutatorParams(TABLE).listener(listener);

        //add by cpeixin 后添加的配置
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.31.63,192.168.31.61,192.168.31.62");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");


        /**
         * step 1: 创建一个连接Connection和BufferedMutator对象，供线程池中的所有线程共享使用
         *              这里利用了jdk1.7里的新特性try(必须实现java.io.Closeable的对象){}catch (Exception e) {},
         *              在调用完毕后会主动调用(必须实现java.io.Closeable的对象)的close()方法，
         *              这里也即默认实现了finally的功能，相当于执行了
         *              finally{
         *              	conn.close();
         *              	mutator.close();
         *              }
         */
        try (
//                final Connection conn = ConnectionFactory.createConnection(getConf());
                final Connection conn = ConnectionFactory.createConnection(conf);
                final BufferedMutator mutator = conn.getBufferedMutator(params)
        ) {
            /** 操作BufferedTable对象的工作线程池，大小为10 */
            final ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
            List<Future<Void>> futures = new ArrayList<>(TASK_COUNT);

            /** 不断创建任务，放入线程池执行，任务数为100个 */
            for (int i = 0; i < TASK_COUNT; i++) {
                futures.add(workerPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        /**
                         * step 2: 所有任务都共同向BufferedMutator的缓冲区发送数据，
                         *              所有任务共享BufferedMutator的缓冲区(hbase.client.write.buffer),
                         *              所有任务共享回调监听器listener和线程池
                         *  */

                        /**
                         * 这里构造Put对象
                         *  */
                        Put p = new Put(Bytes.toBytes("someRow"));
                        p.addColumn(FAMILY, Bytes.toBytes("someQualifier"), Bytes.toBytes("some value"));
                        /**
                         * 添加数据到BufferedMutator的缓冲区(hbase.client.write.buffer)，
                         * 这里不会立即提交数据到hbase服务端，只会在缓冲区大小大于hbase.client.write.buffer时候才会主动提交数据到服务端
                         *  */
                        mutator.mutate(p);

                        /**
                         * TODO
                         * 这里你可以在退出本任务前自己主动调用mutator.flush()提交数据到hbase服务端
                         * mutator.flush();
                         *  */
                        return null;
                    }
                }));
            }

            /**
             * step 3: 遍历每个回调任务的Future，如果未执行完，每个Future等待5分钟
             */
            for (Future<Void> f : futures) {
                f.get(5, TimeUnit.MINUTES);
            }
            /**
             * 最后关闭线程池
             */
            workerPool.shutdown();
        } catch (IOException e) {
            // exception while creating/destroying Connection or BufferedMutator
            LOG.info("exception while creating/destroying Connection or BufferedMutator", e);
        }
        /**
         * 这里没有finally代码，原因是前面用了jdk1.7里的新特性try(必须实现java.io.Closeable的对象){}catch (Exception e) {},
         * 在调用完毕后会主动调用(必须实现java.io.Closeable的对象)的close()方法，也即会调用conn.close(),mutator.close()
         */
        return 0;
    }

    public static void main(String[] args) throws Exception {
        //调用工具类ToolRunner执行实现了接口Tool的对象BufferedMutatorExample的run方法，同时会把String[] args传入BufferedMutatorExample的run方法
        ToolRunner.run(new BufferedMutatorExample(), args);
    }
}