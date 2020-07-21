package com.aikosolar.app;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class DummyKafkaConsumer {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        properties.put("group.id", "all");
        properties.put("enable.auto.commit", true);//设置是否为自动提交
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Arrays.asList("t1", "t2", "t3", "t4"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
                if (!consumerRecords.isEmpty()) {
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        System.out.println(String.format("Topic:%s Partition:%s Offset:%s Msg:%s",
                                consumerRecord.topic(),
                                consumerRecord.partition(),
                                consumerRecord.offset(),
                                consumerRecord.value()
                        ));
                    }
                }
            }
        } catch (Exception e) {
            //处理异常
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(consumer);
        }
    }

}
