import examples.KafkaProducerConsumerRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {
	private static final int WINDOW_DURATION = 10;
	private static final String APP_NAME = "JavaKafkaSparkStreaming";

	// Spark master has to local[n] where n > 1 for receivers to receive data and processors to process data.
	private static final String SPARK_MASTER = "local[2]";

	private static final int BATCH_SIZE_MILLISECONDS = 2000;

	private static final String TABLE_NAME = "logs";

	private static final String SCHEMA_SOURCE = "./src/main/resources/schema.json";

	//Required to be able to update logs within an inner class (VoidFunction that is used in foreachRDD).
	// Explanation here in a different context: http://stackoverflow.com/questions/1299837/cannot-refer-to-a-non-final-variable-inside-an-inner-class-defined-in-a-differen
	// Java's implementation of closure is slightly different where after Main is done it will clear away the local variable
	// so the variable inside the anonmymous inner class may reference to a non-existing variable which is why it needs to final
	// In my scenario, I can't make it final since I need to update my SQL table as streaming data comes in.

	private static StreamHandler streamHandler;
    /**
     * Consumes messages from one or more topics in Kafka and does KMeans clustering.
     *
     * Usage:  Main <zkQuorum> <group> <topics> <numThreads>
     *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
     *   <group> is the name of kafka consumer group
     *   <topics> is a list of one or more kafka topics to consume from
     *   <numThreads> is the number of threads the kafka consumer should use
     */

    public static void main(String[] args) throws IOException {
        KafkaProducerConsumerRunner kafkaProducerConsumerRunner = new KafkaProducerConsumerRunner();

        if (args.length < 4) {
            System.err.println("Usage: Main <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

	    int numThreads = Integer.parseInt(args[3]);
	    Map<String, Integer> topicMap = new HashMap<>();

	    String[] topics = args[2].split(",");
	    for (String topic: topics) {
		    topicMap.put(topic, numThreads);
	    }

        SparkConf sparkConf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APP_NAME);

	    JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sc, new Duration(BATCH_SIZE_MILLISECONDS));

	    SQLContext sqlContext = new SQLContext(sc);

	    StreamHandler streamHandler = new StreamHandler();

	    DataFrame logs = setUpSchemaTableForLogs(sqlContext);

	    //Only output error logs.
	    LogManager.getRootLogger().setLevel(Level.ERROR);

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(javaStreamingContext, args[0], args[1], topicMap);

	    //Transformation and actions for DStreams code here to a format that can be processed by Word2Vec to be able to run KMeans on

	    messages.window(Durations.seconds(WINDOW_DURATION));

	    streamHandler.processStream(messages, logs, sqlContext);

	    javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }

	public static DataFrame setUpSchemaTableForLogs(SQLContext sqlContext) {

		DataFrame logs = sqlContext.read().json(SCHEMA_SOURCE);

		logs.registerTempTable(TABLE_NAME);
		logs.cache();

		return logs;

	}
}
