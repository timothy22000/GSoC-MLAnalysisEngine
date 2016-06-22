import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;

import static org.apache.spark.sql.types.DataTypes.DoubleType;

public class StreamHandler {

	private static DataFrame globalLogs;

	private static DataFrame clusterResults;

	private static ClassificationProcessor classificationProcessor;

	public void processStream(JavaPairReceiverInputDStream<String, String> messages, DataFrame logs, SQLContext sqlContext) {
		globalLogs = logs;

		classificationProcessor = new ClassificationProcessor(100, 000000001);

		/**
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

				createDataframeFromRdd(stringStringJavaPairRDD, sqlContext);

				//Ensure that there is entries in the table.
				if(globalLogs.count() > 0) {
					DataFrame logsForProcessing = sqlContext.sql("SELECT geoip.city_name, verb, response, request FROM logs");

					DataFrame logsForProcessingFixed = logsForProcessing.withColumn("response", globalLogs.col("response").cast(DoubleType));

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

					if(clusterResults != null) {
						classificationProcessor.linearRegressionWithSGD(clusterResults);
					}

				}

			}});
	}

	private void createDataframeFromRdd(JavaPairRDD<String, String> stringStringJavaPairRDD, SQLContext sqlContext) {
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
}
