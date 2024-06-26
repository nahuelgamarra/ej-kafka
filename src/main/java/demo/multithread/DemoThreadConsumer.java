package demo.multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class DemoThreadConsumer extends Thread {
    private final KafkaConsumer<String, String> consumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final static Logger log = LoggerFactory.getLogger(DemoThreadConsumer.class);

    public DemoThreadConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList("curso-topic"));
            try {
                while (!closed.get()) {
                    ConsumerRecords<String, String> consumers = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> consumerRecord : consumers) {

                        log.info("Offset = {}, Partition = {},  Key ={}, Value = {}", consumerRecord.offset(), consumerRecord.partition(), consumerRecord.key(), consumerRecord.value());
                    }
                }
            } catch (WakeupException e) {
              if(!closed.get()){
                  throw e;
              }
            } finally {
                consumer.close();
            }

    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

}
