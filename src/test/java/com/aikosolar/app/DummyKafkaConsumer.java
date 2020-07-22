package com.aikosolar.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class DummyKafkaConsumer {

    public static void main(String[] args) throws Exception {

        Properties properties = new Properties();
//        测试
//        properties.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        properties.put("bootstrap.servers", "172.16.111.21:9092,172.16.111.22:9092,172.16.111.20:9092");
        properties.put("group.id", "al1l");
        properties.put("enable.auto.commit", true);//设置是否为自动提交
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(properties);
//            测试
//            consumer.subscribe(Arrays.asList("t1", "t2", "t3", "t4"));
            consumer.subscribe(Arrays.asList("filebeat"));
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
                if (!consumerRecords.isEmpty()) {
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        String msg = consumerRecord.value();
                        Object hostname = JSONPath.eval(msg,"$.host.name");
                        Object path = JSONPath.eval(msg,"$.log.file.path");
                        Object message = JSONPath.eval(msg,"$.message");
                        // 我只看DF的日志
                        if(path.toString().matches("\\S+[A-Z]{2}\\d{2}-\\d{1,3}\\S+")){
                            System.out.println(String.format("P:%s (%s | %s | %s)",
                                    consumerRecord.partition(),
                                    hostname,
                                    path,
                                    message
                            ));
                        }
//                        测试
//                        System.out.println(String.format("Topic:%s Partition:%s Offset:%s Msg:%s",
//                                consumerRecord.topic(),
//                                consumerRecord.partition(),
//                                consumerRecord.offset(),
//                                consumerRecord.value()
//                        ));
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



class DFLog{

    private String host;

    private String message;
}

