package demo.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TransactionalProducer {
    private static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("transactional.id", "demo-topic-Transactional-id");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "10");
        try (Producer<String, String> producer = new KafkaProducer<>(props);) {
            try {
                producer.initTransactions();
                producer.beginTransaction();

                for (int i = 0; i < 100; i++) {
                    producer.send(new ProducerRecord<>("curso-topic", String.valueOf(i), "demo-msj-transaction"));
                    if (i ==50) {
                  //     throw new Exception("Unexpected Exception");
                    }
                }
                producer.commitTransaction();
                producer.flush();

            } catch (Exception e) {
                log.error("Error: que pasa master", e.getMessage());
                  producer.abortTransaction();
            }
        }
    }
}
