import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Main {

    /**
     * Consumes messages from one or more topics in Kafka and does KMeans clustering.
     *
     * Usage:  Main <zkQuorum> <group> <topics> <numThreads>
     *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
     *   <group> is the name of kafka consumer group
     *   <topics> is a list of one or more kafka topics to consume from
     *   <numThreads> is the number of threads the kafka consumer should use
     */

    public static void main(String[] args) {
        KafkaProducerConsumerRunner kafkaProducerConsumerRunner = new KafkaProducerConsumerRunner();
//        kafkaProducerConsumerRunner.testRun();

        if (args.length < 4) {
            System.err.println("Usage: Main <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

		// Spark master has to local[n] where n > 1 for receivers to receive data and processors to process data.
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaKafkaSparkStreaming");

        // Create the context with 2 seconds batch size
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, new Duration(2000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();

        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(javaStreamingContext, args[0], args[1], topicMap);

	    //Transformation and actions for DStreams code here to a format that can be processed by Word2Vec to be able to run KMeans on

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }
}
