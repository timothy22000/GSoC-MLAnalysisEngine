import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Main {

	//Required to be able to update logs within an inner class (VoidFunction that is used in foreachRDD).
	// Explanation here in a different context: http://stackoverflow.com/questions/1299837/cannot-refer-to-a-non-final-variable-inside-an-inner-class-defined-in-a-differen
	// Java's implementation of closure is slightly different where after Main is done it will clear away the local variable
	// so the variable inside the anonmymous inner class may reference to a non-existing variable which is why it needs to final
	// In my scenario, I can't make it final since I need to update my SQL table as streaming data comes in.
	private static DataFrame globalLogs;

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
//        kafkaProducerConsumerRunner.testRun();

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

	    globalLogs = logs;

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

	    /**\
	     * To Do Tonight:
	     *
	     * i) Alternatively, SparkSQL does not have INSERT INTO built into it. Use RDD to update the SQL table
	     * with new data coming from streams then re-train non-streaming KMeans on that model. (There is problems with this approach
	     * because
	     *
	     * Refer to this for idea:
	     * http://stackoverflow.com/questions/36578936/spark-ml-stringindexer-different-labels-training-testing?rq=1
	     *
	     * ii) Process into DataFrame then find a way to switch columns into Vectors so that streaming KMeans can be trained on it
	     *
	     * iii) Figure out how to convert output from clustering back into categorical
	     *
	     */

	    messages.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
		    @Override
		    public void call(JavaPairRDD<String, String> stringStringJavaPairRDD) throws Exception {
			    if(!stringStringJavaPairRDD.values().isEmpty()){

				    DataFrame streamLog = sqlContext.read().json(stringStringJavaPairRDD.values());

				    globalLogs = globalLogs.unionAll(streamLog);

				    //Need to register the new accumulated logs temp table again. Otherwise global logs will have
				    //a Spark exception Attempted to use BlockRDD at createStream at Main.java:85 after its blocks have been removed!
				    //because you are overwriting globalLogs with a new DataFrame but you have not register it as a temp table yet
				    globalLogs.registerTempTable("logs");
				    globalLogs.cache();

				    System.out.println(globalLogs.count());

				    globalLogs.show();

			    }

		    }

	    });

//	    messages.foreachRDD(stringStringJavaPairRDD -> {
//		    if(!stringStringJavaPairRDD.values().isEmpty()){
//
//			    DataFrame streamLog = sqlContext.read().json(stringStringJavaPairRDD.values());
//
//			    streamLog.show();
//
//			    streamLog.insertInto("logs");
//
//			    logs.show();
//
//		    }

//		    //Extracting nested city name from geoip column only if table has entries.
//		    if(logs.count() > 0) {
//			    DataFrame logsForProcessing = sqlContext.sql("SELECT geoip.city_name, verb, response, request FROM logs");
//
//			    logsForProcessing.printSchema();
//			    logsForProcessing.show();
//
//			    DataFrame logsForProcessingFixed = logsForProcessing.withColumn("response", logs.col("response").cast(DoubleType));
//
//			    logsForProcessingFixed.printSchema();
//			    logsForProcessingFixed.show();
//
//			    /**
//			     * Converting categorical features to numerical features due to how kmeans work.
//			     * (might be more helpful for classification) Try using Word2Vec.
//			     */
//
//			    StringIndexer requestIndex = new StringIndexer().setInputCol("request").setOutputCol("requestIndex");
////			    OneHotEncoder oneHotEncoderRequest = new OneHotEncoder().setInputCol("requestIndex").setOutputCol("requestVec");
//
////
////			    StringIndexer verbIndex = new StringIndexer().setInputCol("verb").setOutputCol("verbIndex");
//
//
//			    //Looks like there is a problem when using geoip
////			    StringIndexer geoIpCityNameIndex = new StringIndexer().setInputCol("city_name").setOutputCol("geoipCityNameIndex");
////
//
//			    VectorAssembler assembler = new VectorAssembler()
//					    .setInputCols(
//							    new String[]{"response", "requestIndex"}
//					    ).setOutputCol("features");
//
////			    Normalizer normalizer = new Normalizer().setInputCol("features").setOutputCol("features_normalized").setP(1);
//
////			    DataFrame logsWithFeaturesNormalized = normalizer.transform(logsWithFeatures);
////			    logsWithFeaturesNormalized.show();
//
////			    org.apache.spark.ml.clustering.KMeans kmeans = new org.apache.spark.ml.clustering.KMeans()
////					    .setK(2)
////					    .setFeaturesCol("features")
////					    .setPredictionCol("prediction");
//
//			    Pipeline pipeline = new Pipeline()
//					    .setStages(new PipelineStage[]{requestIndex, assembler});
//
//			    PipelineModel pipelineModel = pipeline.fit(logsForProcessingFixed);
//
//			    DataFrame logsWithFeatures = pipelineModel.transform(logsForProcessingFixed);
//
//			    logsWithFeatures.show();
//
//		    }

//	    });

	    javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }
}
