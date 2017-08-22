package Moudle.Kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 生产者
 * Created by cluster on 2017/4/10.
 */
public class NewProducer {
    private static final String TOPIC = "cpeixin";
    private static final String CONTENT = "this is send message";
    private static final String BROKER_LIST = "192.168.31.6:9092";
    private static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder";

    public static void main(String args[]){
        Properties props = new Properties();
        props.put("serializer.class",SERIALIZER_CLASS);
        props.put("metadata.broker.list",BROKER_LIST);

        ProducerConfig config = new ProducerConfig(props);
        Producer<String,String> producer = new Producer<String, String>(config);

        //send multiple
//        KeyedMessage<String, String> message =
//                new KeyedMessage<String, String>(TOPIC, CONTENT);

        List<KeyedMessage<String,String>> messages =
                new ArrayList<KeyedMessage<String, String>>();

        for (int i = 0;i<=10;i++){
            messages.add(new KeyedMessage<String, String>(TOPIC,i+","+(int)(Math.random()*100)));
        }
        producer.send(messages);

    }
}
