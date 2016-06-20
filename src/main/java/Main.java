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
	private static DataFrame globalLogs;

	private static DataFrame clusterResults;

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

			    //Ensure that there is entries in the table.
			    if(logs.count() > 0) {
				    DataFrame logsForProcessing = sqlContext.sql("SELECT geoip.city_name, verb, response, request FROM logs");

				    DataFrame logsForProcessingFixed = logsForProcessing.withColumn("response", logs.col("response").cast(DoubleType));

				    DataFrame logsForProcessingRemoveNulls = logsForProcessingFixed.na().drop();

				    logsForProcessingRemoveNulls.printSchema();
				    logsForProcessingRemoveNulls.show();

				    /**
				     * Converting categorical features to numerical features due to how kmeans work.
				     * Two ways:
				     * i) Index features that are categorical one by one with StringIndexer
				     * ii) Combine all categorical feature into a single Vector and then use VectorIndexer
				     *
				     * (i) is preferred in the situation where different categorical features have different number of
				     * categories (Ex: one having 2 categories and one having 100 categories).
				     *
				     */

				    StringIndexer requestIndex = new StringIndexer().setInputCol("request").setOutputCol("requestIndex");
				    //			    OneHotEncoder oneHotEncoderRequest = new OneHotEncoder().setInputCol("requestIndex").setOutputCol("requestVec");


				    StringIndexer verbIndex = new StringIndexer().setInputCol("verb").setOutputCol("verbIndex");


				    //Looks like there is a problem when using geoip
				    StringIndexer geoIpCityNameIndex = new StringIndexer().setInputCol("city_name").setOutputCol("geoIpCityNameIndex");

				    VectorAssembler assembler = new VectorAssembler()
						    .setInputCols(
								    new String[]{"response", "requestIndex", "verbIndex", "geoIpCityNameIndex"}
						    ).setOutputCol("features");

				    Normalizer normalizer = new Normalizer().setInputCol("features").setOutputCol("features_normalized").setP(1);

				    //			    DataFrame logsWithFeaturesNormalized = normalizer.transform(logsWithFeatures);
				    //			    logsWithFeaturesNormalized.show();

				    IndexToString indexToString = new IndexToString().setInputCol("prediction").setOutputCol("predictionOri");

				    org.apache.spark.ml.clustering.KMeans kmeans = new org.apache.spark.ml.clustering.KMeans()
						    .setK(3)
						    .setFeaturesCol("features")
						    .setPredictionCol("clusters");

				    Pipeline pipeline = new Pipeline()
						    .setStages(new PipelineStage[]{requestIndex, verbIndex, geoIpCityNameIndex, assembler});

				    PipelineModel pipelineModel = pipeline.fit(logsForProcessingRemoveNulls);

				    DataFrame logsWithFeatures = pipelineModel.transform(logsForProcessingRemoveNulls);

				    KMeansModel kmeansModel = kmeans.fit(logsWithFeatures);
				    DataFrame logsAfterKMeans = kmeansModel.transform(logsWithFeatures);

				    clusterResults = logsAfterKMeans;

//				    logsAfterKMeans.printSchema();
//				    logsAfterKMeans.show();

				    //Filter rows that have been assigned to each clusters and run descriptive stats on it
//				    logsAfterKMeans.filter("clusters = 0").show();
//				    logsAfterKMeans.filter("clusters = 0").describe().show();
//
//				    logsAfterKMeans.filter("clusters = 1").show();
//				    logsAfterKMeans.filter("clusters = 1").describe().show();

				    for (Vector centre : kmeansModel.clusterCenters()) {
//					    System.out.println(centre);
				    }

				    //Start classification analysis here.

				    //Simple analysis with only one feature.
				    JavaRDD<LabeledPoint> featureLabel = logsAfterKMeans.select(logsAfterKMeans.col("clusters").alias("label"), logsAfterKMeans.col("verbIndex"))
						    .javaRDD().map(new Function<Row, LabeledPoint>() {
							    @Override
							    public LabeledPoint call(Row row) throws Exception {
								    System.out.println("Label " + row.get(0));
								    System.out.println("Features " + row.get(1));
								    return new LabeledPoint( (double) ((Integer) row.get(0)).intValue(), Vectors.dense((double) row.get(1)));
							    }
			        });

				    //Split 40% training data, 60% test data
				    JavaRDD<LabeledPoint>[] splits =
						    featureLabel.randomSplit(new double[]{0.4, 0.6}, 11L);
				    JavaRDD<LabeledPoint> training = splits[0].cache();
				    JavaRDD<LabeledPoint> test = splits[1];

				    //More interesting complex analysis with two or more features.

				    //Linear regression model settings

				    int noOfIterations = 100;
				    double stepSize = 0.00000001;


				    //Train on full data for now. Can slice before doing the map to LabeledPoints
				    LinearRegressionModel linearRegressionModel = LinearRegressionWithSGD.train(JavaRDD.toRDD(training), noOfIterations, stepSize);

				    // Evaluate model on training examples and compute training error
				    JavaRDD<Tuple2<Object, Object>> valuesAndPreds = test.map(
						    new Function<LabeledPoint, Tuple2<Object, Object>>() {
							    public Tuple2<Object, Object> call(LabeledPoint point) {
								    double prediction = linearRegressionModel.predict(point.features());
								    System.out.println("Prediction: " + prediction);
								    return new Tuple2<Object, Object>(prediction, point.label());
							    }
						    }
				    );

//				    double MSE = new JavaDoubleRDD(valuesAndPreds.map(
//						    new Function<Tuple2<Double, Double>, Object>() {
//							    public Object call(Tuple2<Double, Double> pair) {
//								    return Math.pow(pair._1() - pair._2(), 2.0);
//							    }
//						    }
//				    ).rdd()).mean();
//
//				    System.out.println("training Mean Squared Error = " + MSE);

				    //Evaluation step
				    BinaryClassificationMetrics binaryClassificationMetrics = new BinaryClassificationMetrics(valuesAndPreds.rdd(), 0);

				    JavaRDD<Tuple2<Object, Object>> roc = binaryClassificationMetrics.roc().toJavaRDD();

				    System.out.println("ROC curve " + roc.toArray());
				    System.out.println("ROC curve ?????" + binaryClassificationMetrics.areaUnderROC());
		    }

	    }});

	    javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

    }
}
