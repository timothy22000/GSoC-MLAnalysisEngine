import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.maps.model.LatLng;
import geocoder.Geocoder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rulegenerator.RuleGenerator;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;

public class StreamHandler implements Serializable {
	private static final Logger logger = LoggerFactory.getLogger(StreamHandler.class);
	private static final String outputFileDir = "src/main/resources/output";

	private static DataFrame globalLogs;

	private static DataFrame clusterResults;

	private static ClassificationProcessor classificationProcessor;

	private static ClusteringProcessor clusteringProcessor;

	private static Geocoder geocoder = new Geocoder("AIzaSyBRxFmYNrT6kcJqOwWSa4MMwFpcBccIMAU");

	//See Google Guave's Loading Cache for more details.
	private static LoadingCache<String, Optional<LatLng>> cache = CacheBuilder.newBuilder()
			.maximumSize(500)
			.expireAfterAccess(24, TimeUnit.HOURS)
			.recordStats()
			.build(new CacheLoader<String, Optional<LatLng>>(

			       ) {
				       @Override
				       public Optional<LatLng> load(String address) throws Exception {
					       return geocoder.geocode(address);
				       }
			       }
			);;


	//UDF for Spark SQL
	public double geocodeLat(String address) {
		try {
			Optional<LatLng> latLngOptional = cache.get(address);

			if(latLngOptional.isPresent()) {
				return latLngOptional.get().lat;
			}
		} catch (ExecutionException e) {
			logger.error("Cache error - look at logs from cache to identify problem");
		}

		return 0;
	}

	//UDF for Spark SQL
	public double geocodeLong(String address) {
		try {
			Optional<LatLng> latLngOptional = cache.get(address);

			if(latLngOptional.isPresent()) {
				return latLngOptional.get().lng;
			}
		} catch (ExecutionException e) {
			logger.error("Cache error - look at logs from cache to identify problem");
		}
		return 0;
	}

	public void processStream(JavaPairReceiverInputDStream<String, String> messages, DataFrame logs, SQLContext sqlContext, JavaSparkContext sparkContext) {
		globalLogs = logs;

		classificationProcessor = new ClassificationProcessor(10, 0.000000000000001);
		clusteringProcessor = new ClusteringProcessor(5, "features", "clusters");

//		sqlContext.udf().register("geocodeCityCountryLat",  (String string) -> geocodeLat(string), DataTypes.DoubleType);
//		sqlContext.udf().register("geocodeCityCountryLng",  (String string) -> geocodeLong(string), DataTypes.DoubleType);

		/**
		 * Process streaming messages for ML. Steps involved:
		 * i) Extract interested features
		 * ii) Index and vectorize them - Need to add vectorizer and indexer for other features.
		 * iii) Run clustering algorithm on data with the features
		 * iv) Run classification algorithm on data based on the results from clustering algo
		 * v) Generate csv file (per minute)
		 * vi) Generate rule file (per minute)
		 *
		 */

		messages.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {
			@Override
			public void call(JavaPairRDD<String, String> stringStringJavaPairRDD) throws Exception {

				createDataframeFromRdd(stringStringJavaPairRDD, sqlContext);

				//Ensure that there is entries in the table.
				if(globalLogs.count() > 0) {

					//Using logstash's inbuilt geocoder
					DataFrame logsForProcessing = sqlContext.sql("SELECT geoip.city_name, geoip.latitude, geoip.longitude, verb, response, request FROM logs");

					DataFrame logsForProcessingFixed = logsForProcessing.withColumn("response", globalLogs.col("response").cast(DoubleType));

//					//Using google geocoder for lat/long
//					DataFrame logsForProcessingLatLng =  sqlContext.sql("SELECT geocodeCityCountryLat(geoip.city_name) AS lat, geocodeCityCountryLng(geoip.city_name) AS long, geoip.latitude, geoip.longitude, verb, response, request FROM logs");

//					logsForProcessingLatLng.printSchema();
//					logsForProcessingLatLng.show();

					DataFrame logsForProcessingRemoveNulls = logsForProcessingFixed.na().drop();

//					logsForProcessingRemoveNulls.printSchema();
//					logsForProcessingRemoveNulls.show();

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

					Normalizer normalizer = new Normalizer().setInputCol("features").setOutputCol("features_normalized");

					VectorAssembler assembler = new VectorAssembler()
							.setInputCols(
									new String[]{"response", "requestIndex", "verbIndex", "geoIpCityNameIndex"}
							).setOutputCol("features");


					Pipeline pipeline = new Pipeline()
							.setStages(new PipelineStage[]{requestIndex, verbIndex, geoIpCityNameIndex, assembler, normalizer});

					PipelineModel pipelineModel = pipeline.fit(logsForProcessingRemoveNulls);

					DataFrame logsWithFeatures = pipelineModel.transform(logsForProcessingRemoveNulls);

					VectorAssembler assemblerForOneFeature = new VectorAssembler()
							.setInputCols(
									new String[]{"verbIndex"}
							).setOutputCol("features");

					Pipeline pipelineSingleFeature = new Pipeline()
							.setStages(new PipelineStage[]{verbIndex, assemblerForOneFeature, normalizer});

					PipelineModel pipelineModelSingleFeature = pipelineSingleFeature.fit(logsForProcessingRemoveNulls);

					DataFrame logsWithSingleFeature = pipelineModelSingleFeature.transform(logsForProcessingRemoveNulls);

					VectorAssembler assemblerForLatLong = new VectorAssembler()
							.setInputCols(
									new String[]{"latitude", "longitude"}
							).setOutputCol("latLong");

					Pipeline pipelineLatLong = new Pipeline()
							.setStages(new PipelineStage[]{assemblerForLatLong});

					PipelineModel pipelineModelLatLong = pipelineLatLong.fit(logsForProcessingRemoveNulls);

					DataFrame logsWithLatLongSingleFeature= pipelineModelLatLong.transform(logsForProcessingRemoveNulls);

//					logsWithFeatures.printSchema();
//					logsWithFeatures.show();

//					logsWithSingleFeature.printSchema();
//					logsWithSingleFeature.show();

//					logsWithLatLongSingleFeature.printSchema();
//					logsWithLatLongSingleFeature.show();

					KMeansModel kmeansModel = clusteringProcessor.startKMeans(logsWithFeatures);

					clusterResults = clusteringProcessor.getClusterResults();

//					KMeansModel kmeansModelSingleFeature = clusteringProcessor.startKMeans(logsWithSingleFeature);
//
//					clusterResults = clusteringProcessor.getClusterResults();


//					StandardScaler standardScaler = new StandardScaler()
//							.setWithMean(true)
//							.setWithStd(true)
//							.setInputCol("features")
//							.setOutputCol("featuresScaled");
//
//					StandardScalerModel standardScalerModel = standardScaler.fit(clusterResults);
//
//					DataFrame scaledLogsAfterKMeans = standardScalerModel.transform(clusterResults);

//					scaledLogsAfterKMeans.printSchema();
//					scaledLogsAfterKMeans.show();

				    clusterResults.printSchema();
				    clusterResults.show();

					RuleGenerator ruleGenerator = new RuleGenerator();

					if(clusterResults != null) {

						//Logistic Regression Simple
//						JavaRDD<Tuple2<Object, Object>>  valueAndPredsLogisticReg = classificationProcessor.logisticRegressionWithLgbtSimple(clusterResults);
//						classificationProcessor.computeMeanSquaredError(valueAndPredsLogisticReg);
//						classificationProcessor.evaluateRoc(valueAndPredsLogisticReg);
//						ConcurrentHashMap<String, Double> metrics = classificationProcessor.calculateMetricsForLogisticRegression(valueAndPredsLogisticReg);
//
//						DataFrame clusterResultsWithPrecision = clusterResults.withColumn("precision", functions.lit(metrics.get("precision")));
//						clusterResultsWithPrecision.printSchema();
//						String fileName = createRuleCsvFile(clusterResultsWithPrecision);
//
//						ruleGenerator.generateRuleFile(fileName);

						//Logistic Regression Complex
						JavaRDD<Tuple2<Object, Object>>  valueAndPredsLogisticRegComplex = classificationProcessor.logisticRegressionWithLgbtComplex(clusterResults);
						classificationProcessor.computeMeanSquaredError(valueAndPredsLogisticRegComplex);
						classificationProcessor.evaluateRoc(valueAndPredsLogisticRegComplex);
						ConcurrentHashMap<String, Double> metrics = classificationProcessor.calculateMetricsForLogisticRegression(valueAndPredsLogisticRegComplex);

						DataFrame clusterResultsWithPrecision = clusterResults.withColumn("precision", functions.lit(metrics.get("precision")));
						clusterResultsWithPrecision.printSchema();
						String fileName = createRuleCsvFile(clusterResultsWithPrecision);

						ruleGenerator.generateRuleFile(fileName);

						//Naive Bayes Simple
//						JavaPairRDD<Double, Double> valueAndPredsNaiveBayesSimple = classificationProcessor.naiveBayesSimple(clusterResults);

						//Naive Bayes Complex
//						JavaPairRDD<Double, Double> valueAndPredsNaiveBayesComplex = classificationProcessor.naiveBayesComplex(clusterResults);


						if(clusterResults.count() > 2) {
							//Decision Tree Simple
//							DataFrame valueAndPredsDecisionTreeSimple = classificationProcessor.decisionTreeSimple(clusterResults);
//							classificationProcessor.evaluatePrecisionDecisionTrees(valueAndPredsDecisionTreeSimple);
//							classificationProcessor.evaluateRecallDecisionTrees(valueAndPredsDecisionTreeSimple);

							//Decision Tree Complex
//							DataFrame valueAndPredsDecisionTreeComplex = classificationProcessor.decisionTreeComplex(clusterResults);
////							classificationProcessor.evaluateAccuracyDecisionTrees(valueAndPredsDecisionTreeComplex);
//							classificationProcessor.evaluatePrecisionDecisionTrees(valueAndPredsDecisionTreeComplex);
//							classificationProcessor.evaluateRecallDecisionTrees(valueAndPredsDecisionTreeComplex);
						}


					}

				}

			}});
	}

	private String createRuleCsvFile(DataFrame clusterResultsWithPrecision) throws IOException {
		//Collate output per minute from streaming data
		String fileName = "oneFeatureVerbRuleCsv--" + new SimpleDateFormat("yyyy-MM-dd--HH-mm").format(new Date());

		//Repartition is used to create only one csv files instead of multiple parts.
		clusterResultsWithPrecision.select("verb", "response", "requestIndex", "clusters", "precision").
				repartition(1).
				write().
				format("com.databricks.spark.csv").
				option("header", "true").
				mode(SaveMode.Overwrite).
				save(fileName);


		moveFile(new File(fileName + "/part-00000"), new File("src/main/resources/output/" + fileName));
		logger.info("CSV file moving completed");
		System.out.println("CSV file moving completed");
		FileUtils.deleteDirectory(new File(fileName));

		return fileName;
	}

	private boolean moveFile(File origfile, File destfile) {
		boolean fileMoved = false;
		try{
			File newfile = new File(destfile.getParent() + File.separator + destfile.getName());

			if(newfile.exists()) {
				newfile.delete();
			}
			FileUtils.copyFile(origfile, destfile , true);


			if(newfile.exists() && FileUtils.contentEqualsIgnoreEOL(origfile,newfile,"UTF-8"))
			{
				origfile.delete();
				fileMoved = true;
			}
			else
			{
				System.out.println("File fail to move successfully!");
			}
		} catch(IOException e) {
			logger.info(e.toString() + "Details: " + e.getMessage());

		}
		return fileMoved;
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
