package com.aikosolar.app;

import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class DummyKafkaProducer {
    public static void main(String[] args) throws Exception {
        String topic = "t1";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        properties.put("acks", "all");
        properties.put("retries", "0");
        properties.put("batch.size", "16384");
        properties.put("linger.ms", "1");
        properties.put("buffer.memory", "33554432");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");



        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer(properties);
            List<String> messages = Arrays.asList(
                    "07/20/2020 00:50:58 R Version: /PROCESS/IDLE;29 /LIBRARY/STANDARD;50 /LIBRARY/XLPPOCL1;113 /LIBRARY/XLPPOCL2;76 /LIBRARY/XTEMP6Z1;18 /LIBRARY/XTEMP6Z2;8",
                    "07/20/2020 00:50:58 R Recipe Start Recipe:      /PROCESS/IDLE;29 Type:        Standby User:        DF01-L Run no. :    1277 Comment:     Standby recipe",
                    "07/20/2020 00:51:03 L Recipe Int1 State 1",
                    "07/20/2020 00:51:22 R Recipe End Recipe:      /PROCESS/IDLE;29 Type:        Standby User:        DF01-L Run no. :    1277 Runtime:     00:06:32 State:       Ok",
                    "07/20/2020 00:51:30 R Recipe Start Recipe:      /PROCESS/IDLE;29 Type:        Standby User:        DF01-L Run no. :    1277 Comment:     Standby recipe",
                    "07/20/2020 00:51:40 R 000",
                    "07/20/2020 00:51:50 R AAA",
                    "07/20/2020 00:52:30 R BBB",
                    "07/20/2020 00:52:35 R CCC",
                    "07/20/2020 00:52:50 R DDD",
                    "07/20/2020 00:57:50 R DDD",
                    "07/20/2020 00:58:50 R DDD",
                    "07/20/2020 01:01:50 R DDD"
                    );
            for (String m : messages){
                producer.send(new ProducerRecord<>(topic, m));
                System.out.println(m);
                Thread.sleep(3 * 1000);
            }
        } finally {
            IOUtils.closeQuietly(producer);
        }
    }

}
