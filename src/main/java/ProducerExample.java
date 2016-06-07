import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class ProducerExample {
    private Producer producer = createExampleProducer();

    private Properties createDefaultProducerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;

    }

    private Producer createExampleProducer() {
        Producer<String, String> producer = new KafkaProducer<>(createDefaultProducerConfig());

        return producer;
    }

    public Producer getProducer() {
        return producer;
    }
}
