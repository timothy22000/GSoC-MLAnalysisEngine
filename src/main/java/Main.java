import converter.JsonProcessor;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.List;
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

	    //Only output error logs.
	    LogManager.getRootLogger().setLevel(Level.ERROR);

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();

        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(javaStreamingContext, args[0], args[1], topicMap);

	    //Transformation and actions for DStreams code here to a format that can be processed by Word2Vec to be able to run KMeans on

	    messages.window(Durations.seconds(10));

	    messages.foreachRDD(stringStringJavaPairRDD -> {
		    SQLContext sqlContext = SQLContext.getOrCreate(stringStringJavaPairRDD.context());

		    JsonProcessor jsonProcessor = new JsonProcessor();

		    //Using this processing to obtain nested fields in JSON
//		    List<Map<String, String>> mapsOfFlattenJsonObjects = stringStringJavaPairRDD.values().map(s -> {
//			    return jsonProcessor.parseJson(s);
//		    }).reduce(new Function2<List<Map<String, String>>, List<Map<String, String>>, List<Map<String, String>>>() {
//			              @Override
//			              public List<Map<String, String>> call(List<Map<String, String>> maps, List<Map<String, String>> maps2) throws Exception {
//				              maps.addAll(maps2);
//				              return maps;
//			              }
//		              }
//
//		    );

		    DataFrame logs = sqlContext.read().json(stringStringJavaPairRDD.values());

		    //Allows use of SQL commands
		    logs.registerTempTable("logs");
		    logs.printSchema();
		    logs.show();

		    //Extracting nested city name from geoip column only if table has entries.
		    if(logs.count() > 0) {
			    DataFrame geoIpCityName = sqlContext.sql("SELECT geoip.city_name FROM logs");

			    geoIpCityName.show();
		    }

		    /**
		     * Converting categorical features to numerical features due to how kmeans work
		     */


	    });

        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }
}
