package com.spark2.module.spark.Kafka;

/**
 * Created by cluster on 2017/4/10.
 */
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Hello world!
 *
 */
public class KafkaProducer
{
    private final Producer<String, String> producer;
    private static final String TOPIC = "cpeixin";
    private static final String CONTENT = "this is send message";
    private static final String BROKER_LIST = "process1.pd.dp:6667,process6.pd.dp:6667,process7.pd.dp:6667";
    private static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder";
    private static final String KAFKA_SERIALIZER_ENCODER = "kafka.serializer.StringEncoder";

    private KafkaProducer(){
        Properties props = new Properties();
        //此处配置的是kafka的端口
        props.put("metadata.broker.list", BROKER_LIST);
        //配置value的序列化类
        props.put("serializer.class", SERIALIZER_CLASS);
        //配置key的序列化类
        props.put("key.serializer.class", KAFKA_SERIALIZER_ENCODER);
        props.put("request.required.acks","-1");
        producer = new Producer<String, String>(new ProducerConfig(props));
    }
    void produce() {
        int messageNo = 1;
        final int COUNT = 11;

        while (messageNo < COUNT) {
            String msg = messageNo+","+(int)(Math.random()*100);
            producer.send(new KeyedMessage<String, String>(TOPIC,msg));
            System.out.println(msg);
            messageNo ++;
        }
    }

    public static void main( String[] args )
    {
        new KafkaProducer().produce();
    }
}
