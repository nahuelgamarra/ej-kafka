package demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class DemoProducer {
    public static void main(String[] args) {
        Properties props= new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        try (Producer<String,String> producer = new KafkaProducer<>(props);){
            for (int i= 0;i<100; i++){
                producer.send(
                        new ProducerRecord<>("curso-topic", (i%2==0)? "key-par": "key-impar","demo-msj-ordenada "+ i)
                );
            }
            producer.flush();
        }

    }
}
