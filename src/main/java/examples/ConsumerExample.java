package examples;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class ConsumerExample {
    private Consumer consumer = createDefaultConsumer();
    private static final int NO_OF_THREADS = 1;

    private Properties createDefaultConsumerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return props;

    }

    private Consumer<String, String> createDefaultConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createDefaultConsumerConfig());
        return consumer;
    }

    public Consumer getConsumer() {
        return consumer;
    }
}
