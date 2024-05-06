package demo.callbacks;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class DemoCallbackProducer {
    private static final Logger log = LoggerFactory.getLogger(DemoCallbackProducer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        try (Producer<String, String> producer = new KafkaProducer<>(props);) {
            for (int i = 0; i < 1000000; i++) {
                producer.send(new ProducerRecord<>("curso-topic", String.valueOf(i), "demo-msj"),
                        ((metadata, exception) -> {
                    if (exception != null) {
                        log.error("There was an error {}", exception.getMessage());
                    }
                    log.info("Offset = {}, Partition = {}, Topic = {}", metadata.offset(), metadata.partition(), metadata.topic());
                }));

            }
            producer.flush();
        }

    }
}
