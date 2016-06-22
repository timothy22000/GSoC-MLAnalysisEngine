import examples.KafkaProducerConsumerRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.types.DataTypes.DoubleType;

public class Main {

	//Required to be able to update logs within an inner class (VoidFunction that is used in foreachRDD).
	// Explanation here in a different context: http://stackoverflow.com/questions/1299837/cannot-refer-to-a-non-final-variable-inside-an-inner-class-defined-in-a-differen
	// Java's implementation of closure is slightly different where after Main is done it will clear away the local variable
	// so the variable inside the anonmymous inner class may reference to a non-existing variable which is why it needs to final
	// In my scenario, I can't make it final since I need to update my SQL table as streaming data comes in.


	private static ClassificationProcessor classificationProcessor;

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

		// Spark master has to local[n] where n > 1 for receivers to receive data and processors to process data.
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaKafkaSparkStreaming");

	    JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Create the context with 2 seconds batch size
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sc, new Duration(2000));

	    SQLContext sqlContext = new SQLContext(sc);

	    //Infer schema from sample json that will be updated as new RDD comes in. Has to be one line JSON.
		DataFrame logs = sqlContext.read().json("./src/main/resources/schema.json");

	    logs.registerTempTable("logs");
	    logs.cache();

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

	    streamHandler.processStream(messages, logs, sqlContext);

	    javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }


}
