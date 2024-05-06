package demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class DemoConsumer {

    private static final Logger log = LoggerFactory.getLogger(DemoConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "curso-topic-group");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //para hacer que el consumer haga el commit de manera manual
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Arrays.asList("curso-topic"));
            while (true) {
                ConsumerRecords<String, String> consumers = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> consumerRecord : consumers) {
                    log.info("Offset = {}, Partition = {},  Key ={}, Value = {}"
                            , consumerRecord.offset()
                            , consumerRecord.partition()
                            , consumerRecord.key()
                            , consumerRecord.value() );
                    consumer.commitAsync();//para hacer commit manual
                }
            }
        }
    }
}
