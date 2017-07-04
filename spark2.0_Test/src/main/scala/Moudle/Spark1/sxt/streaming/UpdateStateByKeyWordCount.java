//package Moudle.Spark1.sxt.streaming;
//
//import java.util.Arrays;
//import java.util.List;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//import com.google.common.base.Optional;
//
//import scala.Tuple2;
//
//public class UpdateStateByKeyWordCount {
//
//	public static void main(String[] args) {
//		SparkConf conf = new SparkConf().setAppName("wordcount").setMaster("local[2]");
//		JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(5));
//
//		// 第一点,如果要使用updateStateByKey算子,就必须设置一个checkpoint目录,开启checkpoint机制
//		//因为，updateStateByKey是更新操作，而且在输入新数据的时候，还要保留原数据的状态，spark会将上一状态的数据保留在spark内存中
//		//但是，保存在内存中并不是安全的，所以我们必须使用checkpoint来备份数据。
//		//所以在使用updateStateByKey算子的前提就是设置checkpoint dir。。。
//		jssc.checkpoint("hdfs://spark001:9000/wordcount_checkpoint");
//
//		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("spark001", 8888);
//
//		JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>(){
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Iterable<String> call(String line) throws Exception {
//				return Arrays.asList(line.split(" "));
//			}
//
//		});
//
//		JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>(){
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Tuple2<String, Integer> call(String word) throws Exception {
//				return new Tuple2<String, Integer>(word, 1);
//			}
//
//		});
//
//		// updateStateByKey,就可以实现直接通过spark维护一份每个单词的全局的统计次数
//		JavaPairDStream<String, Integer> wordcounts = pairs.updateStateByKey(
//
//				// 这里的Optional,相当于scala中的样例类,就是Option,可以理解它代表一个状态,可能之前存在,也可能之前不存在
//				//参数：
//				//List<Integer>  每一批次里面，key对应的所有值
//				//Optional<Integer> 第二个参数： 原始数据上一状态的值
//				//Optional<Integer> 第三个参数： 新数据注入后，返回的值
//				//Optional有两个子类，一个是，当有值的时候，将值放入some(),并返回some。另一个是，无值的时候，返回none
//				new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>(){
//
//					private static final long serialVersionUID = 1L;
//
//					// 实际上,对于每个单词,每次batch计算的时候,都会调用这个函数,第一个参数,values相当于这个batch中,这个key的新的值,
//					// 可能有多个,比如在输入的数据中，有两个hello单词，一个hello是计数1次,有2个1,(hello,1) (hello,1) 那么传入的是(1,1)，所以value的值是（1，1）
//					// 那么第二个参数表示的是这个key之前的状态,其实泛型的参数是你自己指定的
//					@Override
//					public Optional<Integer> call(List<Integer> values,	Optional<Integer> state) throws Exception {
//						// 定义一个全局的计数
//						//下面的这个判断，只有 有历史记录的数据才能进入下面的循环
//						Integer newValue = 0;
//						if(state.isPresent()){
//							newValue = state.get();
//						}
//						for(Integer value : values){
//							newValue += value;
//						}
//						return Optional.of(newValue);
//					}
//		});
//
//		wordcounts.print();
//
//		jssc.start();
//		jssc.awaitTermination();
//		jssc.close();
//	}
//}
