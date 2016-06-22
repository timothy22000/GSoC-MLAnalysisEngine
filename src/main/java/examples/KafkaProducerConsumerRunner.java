package examples;

import com.cedarsoftware.util.io.JsonWriter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Date;
import java.util.Random;

public class KafkaProducerConsumerRunner {
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(KafkaProducerConsumerRunner.class);

    public void testRun() {
        Random random = new Random();

        ProducerExample producerExample = new ProducerExample();
        Producer producer = producerExample.getProducer();

        ConsumerExample consumerExample = new ConsumerExample();
        Consumer consumer = consumerExample.getConsumer();

        consumer.subscribe(Arrays.asList("test-topic-2", "logstash_logs"));

        for(int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("test-topic", Integer.toString(i), Integer.toString(i)));

            long runtime = new Date().getTime();
            String ip = "192.168.2." + random.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;

            producer.send(new ProducerRecord<String, String>("test-topic-2", ip, msg));
        }

        producer.close();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
//                System.out.printf("offset = %d, topic = %s, key = %s, value = %s", record.offset(), record.topic(), record.key(), record.value());
                String niceFormattedJson = JsonWriter.formatJson(record.value());
                System.out.println(niceFormattedJson);
            }

        }
    }
}
