import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.tree.DecisionTreeModel;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class ClassificationProcessor implements Serializable {
	private int noOfIterations;
	private double stepSize;

	public ClassificationProcessor(int noOfIterations, double stepSize) {
		this.noOfIterations = checkNotNull(noOfIterations);
		this.stepSize = checkNotNull(stepSize);
	}

	public JavaRDD<Tuple2<Object, Object>> linearRegressionWithSdgSimple(DataFrame logsAfterKMeans) {
		//Simple analysis with only one feature.
		JavaRDD<LabeledPoint> featureLabel = logsAfterKMeans.select(
				logsAfterKMeans.col("clusters").alias("label"),
				logsAfterKMeans.col("verbIndex"))
				.javaRDD().map(new Function<Row, LabeledPoint>() {
					@Override
					public LabeledPoint call(Row row) throws Exception {
						System.out.println("Label " + row.get(0));
						System.out.println("Features " + row.get(1));
						return new LabeledPoint( (double) ((Integer) row.get(0)).intValue(), Vectors.dense((double) row.get(1)));
					}
				});

		//Split 40% training data, 60% test data
		JavaRDD<LabeledPoint>[] splits = splitData(featureLabel, 0.4, 0.6, 11L);
		JavaRDD<LabeledPoint> training = splits[0].cache();
		JavaRDD<LabeledPoint> test = splits[1];

		//More interesting complex analysis with two or more features.

		//Train on full data for now. Can slice before doing the map to LabeledPoints
		LinearRegressionModel linearRegressionModel = new LinearRegressionWithSGD().train(
				JavaRDD.toRDD(training),
				noOfIterations,
				stepSize
		);

		// Evaluate model on training examples and compute training error
		JavaRDD<Tuple2<Object, Object>> valuesAndPreds = predictLabelOfTestDataFromLinearRegression(test, linearRegressionModel);

		return valuesAndPreds;
	}

	public JavaRDD<Tuple2<Object, Object>> linearRegressionWithSdgComplex(DataFrame logsAfterKMeans) {

		JavaRDD<LabeledPoint> featureLabel = logsAfterKMeans.select(
				logsAfterKMeans.col("clusters").alias("label"),
				logsAfterKMeans.col("features_normalized")
				)
				.javaRDD().map(new Function<Row, LabeledPoint>() {
					@Override
					public LabeledPoint call(Row row) throws Exception {

						System.out.println("Label " + row.get(0));
						System.out.println("Features " + row.get(1));
						return new LabeledPoint(
								(double) ((Integer) row.get(0)).intValue(),
								(Vector) row.get(1)
						);
					}
				});

		//Split 40% training data, 60% test data
		JavaRDD<LabeledPoint>[] splits = splitData(featureLabel, 0.4, 0.6, 11L);
		JavaRDD<LabeledPoint> training = splits[0].cache();
		JavaRDD<LabeledPoint> test = splits[1];

		//More interesting complex analysis with two or more features.

		//Train on full data for now. Can slice before doing the map to LabeledPoints
		LinearRegressionWithSGD linearRegression = new LinearRegressionWithSGD();

		/**
		 * If intercept is not set to true, it will take 0 as an intercept.
		 * http://stackoverflow.com/questions/26259743/spark-mllib-linear-regression-model-intercept-is-always-0-0
		 */

		linearRegression.setIntercept(true);

		LinearRegressionModel linearRegressionModel = linearRegression.train(
				JavaRDD.toRDD(training),
				noOfIterations,
				stepSize
		);

		// Evaluate model on training examples and compute training error
		JavaRDD<Tuple2<Object, Object>> valuesAndPreds = predictLabelOfTestDataFromLinearRegression(test, linearRegressionModel);

		return valuesAndPreds;
	}

	public JavaRDD<Tuple2<Object, Object>> logisticRegressionWithLgbtSimple(DataFrame logsAfterKMeans) {

		/**
		 * Logistic Regression with LGBT is preferred over SGD
		 * Ref: https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/mllib/classification/package-summary.html
		 */

		JavaRDD<LabeledPoint> featureLabel = logsAfterKMeans.select(
				logsAfterKMeans.col("clusters").alias("label"),
				logsAfterKMeans.col("verbIndex")
		)
				.javaRDD().map(new Function<Row, LabeledPoint>() {
					@Override
					public LabeledPoint call(Row row) throws Exception {

						System.out.println("Label " + row.get(0));
						System.out.println("Features " + row.get(1));
						return new LabeledPoint(
								(double) ((Integer) row.get(0)).intValue(),
								Vectors.dense((double) row.get(1))
						);
					}
				});

		//Split 40% training data, 60% test data
		JavaRDD<LabeledPoint>[] splits = splitData(featureLabel, 0.4, 0.6, 11L);
		JavaRDD<LabeledPoint> training = splits[0].cache();
		JavaRDD<LabeledPoint> test = splits[1];

		LogisticRegressionModel logisticRegressionModel = new LogisticRegressionWithLBFGS()
				.setNumClasses(3)
				.run(training.rdd());

		// Compute raw scores on the test set.
		JavaRDD<Tuple2<Object, Object>> valuesAndPreds = test.map(
				new Function<LabeledPoint, Tuple2<Object, Object>>() {
					public Tuple2<Object, Object> call(LabeledPoint point) {
						Double prediction = logisticRegressionModel.predict(point.features());
						System.out.println("Prediction: " + prediction);
						return new Tuple2<Object, Object>(prediction, point.label());
					}
				}
		);

		return valuesAndPreds;

	}

	public JavaRDD<Tuple2<Object, Object>> logisticRegressionWithLgbtComplex(DataFrame logsAfterKMeans) {

		/**
		 * Logistic Regression with LGBT is preferred over SGD
		 * Ref: https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/mllib/classification/package-summary.html
		 */

		JavaRDD<LabeledPoint> featureLabel = logsAfterKMeans.select(
				logsAfterKMeans.col("clusters").alias("label"),
				logsAfterKMeans.col("features")
		)
				.javaRDD().map(new Function<Row, LabeledPoint>() {
					@Override
					public LabeledPoint call(Row row) throws Exception {

						System.out.println("Label " + row.get(0));
						System.out.println("Features " + row.get(1));
						return new LabeledPoint(
								(double) ((Integer) row.get(0)).intValue(),
								(Vector) row.get(1)
						);
					}
				});

		//Split 40% training data, 60% test data
		JavaRDD<LabeledPoint>[] splits = splitData(featureLabel, 0.4, 0.6, 11L);
		JavaRDD<LabeledPoint> training = splits[0].cache();
		JavaRDD<LabeledPoint> test = splits[1];

		LogisticRegressionModel logisticRegressionModel = new LogisticRegressionWithLBFGS()
				.setNumClasses(3)
				.run(training.rdd());

		// Compute raw scores on the test set.
		JavaRDD<Tuple2<Object, Object>> valuesAndPreds = test.map(
				new Function<LabeledPoint, Tuple2<Object, Object>>() {
					public Tuple2<Object, Object> call(LabeledPoint point) {
						Double prediction = logisticRegressionModel.predict(point.features());
						System.out.println("Prediction: " + prediction);
						return new Tuple2<Object, Object>(prediction, point.label());
					}
				}
		);

		return valuesAndPreds;

	}

	public JavaPairRDD<Double, Double> naiveBayesSimple(DataFrame logsAfterKMeans) {

		JavaRDD<LabeledPoint> featureLabel = logsAfterKMeans.select(
				logsAfterKMeans.col("clusters").alias("label"),
				logsAfterKMeans.col("verbIndex")
		)
				.javaRDD().map(new Function<Row, LabeledPoint>() {
					@Override
					public LabeledPoint call(Row row) throws Exception {

						System.out.println("Label " + row.get(0));
						System.out.println("Features " + row.get(1));
						return new LabeledPoint(
								(double) ((Integer) row.get(0)).intValue(),
								Vectors.dense((double) row.get(1))
						);
					}
				});


		//Split 40% training data, 60% test data
		JavaRDD<LabeledPoint>[] splits = splitData(featureLabel, 0.4, 0.6, 11L);
		JavaRDD<LabeledPoint> training = splits[0].cache();
		JavaRDD<LabeledPoint> test = splits[1];

		NaiveBayesModel naiveBayesModel = NaiveBayes.train(training.rdd(), 1.0);

		JavaPairRDD<Double, Double> valuesAndPreds =
				test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
					@Override
					public Tuple2<Double, Double> call(LabeledPoint point) {
						double prediction = naiveBayesModel.predict(point.features());
						System.out.println("Prediction: " + prediction);
						return new Tuple2<>(prediction, point.label());
					}
				});

		double accuracy = valuesAndPreds.filter(new Function<Tuple2<Double, Double>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Double, Double> doubleDoubleTuple2) throws Exception {
				return doubleDoubleTuple2._1().equals(doubleDoubleTuple2._2());
			}
		}).count() / (double) test.count();

		System.out.println("Naive Bayes Accuracy :" + accuracy);

		return valuesAndPreds;
	}

	public JavaPairRDD<Double, Double> naiveBayesComplex(DataFrame logsAfterKMeans) {

		JavaRDD<LabeledPoint> featureLabel = logsAfterKMeans.select(
				logsAfterKMeans.col("clusters").alias("label"),
				logsAfterKMeans.col("features")
		)
				.javaRDD().map(new Function<Row, LabeledPoint>() {
					@Override
					public LabeledPoint call(Row row) throws Exception {

						System.out.println("Label " + row.get(0));
						System.out.println("Features " + row.get(1));
						return new LabeledPoint(
								(double) ((Integer) row.get(0)).intValue(),
								(Vector) row.get(1)
						);
					}
				});

		//Split 40% training data, 60% test data
		JavaRDD<LabeledPoint>[] splits = splitData(featureLabel, 0.4, 0.6, 11L);
		JavaRDD<LabeledPoint> training = splits[0].cache();
		JavaRDD<LabeledPoint> test = splits[1];

		NaiveBayesModel naiveBayesModel = NaiveBayes.train(training.rdd(), 1.0);

		JavaPairRDD<Double, Double> valuesAndPreds =
				test.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
					@Override
					public Tuple2<Double, Double> call(LabeledPoint point) {
						double prediction = naiveBayesModel.predict(point.features());
						System.out.println("Prediction: " + prediction);
						return new Tuple2<>(prediction, point.label());
					}
				});

		double accuracy = valuesAndPreds.filter(new Function<Tuple2<Double, Double>, Boolean>() {
			@Override
			public Boolean call(Tuple2<Double, Double> doubleDoubleTuple2) throws Exception {
				return doubleDoubleTuple2._1().equals(doubleDoubleTuple2._2());
			}
		}).count() / (double) test.count();

		System.out.println("Naive Bayes Accuracy :" + accuracy);

		return valuesAndPreds;
	}

//	public JavaRDD<Tuple2<Object, Object>> linearRegressionWithElasticNet(DataFrame logsAfterKMeans) {
//
//		/**
//		 * Elastic Net uses L1 and L2 regularization (Typically in SGD, it uses no regularization in Spark implementation)
//		 * Ref: https://spark.apache.org/docs/1.6.2/api/java/org/apache/spark/mllib/regression/package-summary.html
//		 * Need to email Spark mailing list to confirm differences between the different Linear Regression Implementations
//		 */
//
//		LinearRegression linearRegression = new LinearRegression()
//				.setMaxIter(10)
//				.setRegParam(0.3)
//				.setElasticNetParam(0.8)
//				.setFeaturesCol("features")
//				.setLabelCol("label");
//
//		org.apache.spark.ml.regression.LinearRegressionModel linearRegressionModel = linearRegression.fit(logsAfterKMeans);
//
//		System.out.println("Weights: " + linearRegressionModel.weights() + " Intercept: " + linearRegressionModel.intercept());
//
//		return null;
//
//	}

	/**
	 * Non-linear method
	 */

	public DataFrame decisionTreeSimple(DataFrame logsAfterKMeans) {

		DataFrame logsAfterKMeansStringLabel = logsAfterKMeans.withColumn("clusters", logsAfterKMeans.col("clusters").cast(StringType));

		/**
		 * Workaround since the ML version of DT does not have a way to specify the number of classes and the exception
		 * suggests the use of StringIndexer so I converted the labels to String and then index it to Double
		 * rather than converting to Double directly.
		 *
		 * Exception: DecisionTreeClassifier was given input with invalid label column clusters, without the number
		 * of classes specified. See StringIndexer.
		 */

		StringIndexer stringIndexer = new StringIndexer().setInputCol("clusters").setOutputCol("label");

		StringIndexerModel stringIndexerModel = stringIndexer.fit(logsAfterKMeansStringLabel);

		DataFrame logsAfterKMeansFixed = stringIndexerModel.transform(logsAfterKMeansStringLabel);

		//Split 40% training data, 60% test data
		DataFrame[] splits = logsAfterKMeansFixed.randomSplit(new double[]{40, 60}, 11L);
		DataFrame training = splits[0].cache();
		DataFrame test = splits[1];

		DecisionTreeClassifier decisionTreeClassifier = new DecisionTreeClassifier()
				.setLabelCol("label")
				.setFeaturesCol("features");

		//Train and fit model
		DecisionTreeClassificationModel decisionTreeModel = decisionTreeClassifier.fit(training);

		DataFrame prediction = decisionTreeModel.transform(test);

		prediction.select("prediction", "label", "features").show();

		prediction.select("prediction", "label", "features").where("label = 1").show();

		return prediction;

	}

	public DataFrame decisionTreeComplex(DataFrame logsAfterKMeans) {

		DataFrame logsAfterKMeansStringLabel = logsAfterKMeans.withColumn("clusters", logsAfterKMeans.col("clusters").cast(StringType));

		/**
		 * Workaround since the ML version of DT does not have a way to specify the number of classes and the exception
		 * suggests the use of StringIndexer so I converted the labels to String and then index it to Double
		 * rather than converting to Double directly.
		 *
		 * Exception: DecisionTreeClassifier was given input with invalid label column clusters, without the number
		 * of classes specified. See StringIndexer.
		 */

		StringIndexer stringIndexer = new StringIndexer().setInputCol("clusters").setOutputCol("label");

		StringIndexerModel stringIndexerModel = stringIndexer.fit(logsAfterKMeansStringLabel);

		DataFrame logsAfterKMeansFixed = stringIndexerModel.transform(logsAfterKMeansStringLabel);

		//Split 40% training data, 60% test data
		DataFrame[] splits = logsAfterKMeansFixed.randomSplit(new double[]{40, 60}, 11L);
		DataFrame training = splits[0].cache();
		DataFrame test = splits[1];

		DecisionTreeClassifier decisionTreeClassifier = new DecisionTreeClassifier()
				.setLabelCol("label")
				.setFeaturesCol("features")
				.setMaxBins(1000)
				.setMaxDepth(10);

		//Train and fit model
		DecisionTreeClassificationModel decisionTreeModel = decisionTreeClassifier.fit(training);

		DataFrame prediction = decisionTreeModel.transform(test);

		prediction.select("prediction", "label", "features").show();

		return prediction;

	}

	public void evaluateAccuracyDecisionTrees(DataFrame prediction) {
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol("label")
				.setPredictionCol("prediction")
				.setMetricName("accuracy");

		double accuracy = evaluator.evaluate(prediction);

		System.out.println("Decision Tree Accuracy :" + accuracy);
	}

	public void evaluatePrecisionDecisionTrees(DataFrame prediction) {
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol("label")
				.setPredictionCol("prediction")
				.setMetricName("precision");

		double precision = evaluator.evaluate(prediction);

		System.out.println("Decision Tree Precision :" + precision);
	}

	public void evaluateRecallDecisionTrees(DataFrame prediction) {
		MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
				.setLabelCol("label")
				.setPredictionCol("prediction")
				.setMetricName("recall");

		double recall = evaluator.evaluate(prediction);

		System.out.println("Decision Tree Recall :" + recall);
	}

	public JavaRDD<Tuple2<Object, Object>> evaluateRoc(JavaRDD<Tuple2<Object, Object>> valuesAndPreds) {
		//Evaluation step
		BinaryClassificationMetrics binaryClassificationMetrics = new BinaryClassificationMetrics(valuesAndPreds.rdd(), 0);

		JavaRDD<Tuple2<Object, Object>> roc = binaryClassificationMetrics.roc().toJavaRDD();

		System.out.println("ROC curve: " + roc.toArray());
		System.out.println("Area under ROC curve:" + binaryClassificationMetrics.areaUnderROC());

		return roc;
	}

	public void calculateMetricsForLogisticRegression(JavaRDD<Tuple2<Object, Object>> valuesAndPreds) {
		MulticlassMetrics metrics = new MulticlassMetrics(valuesAndPreds.rdd());

		double recall = metrics.recall();
		double precision = metrics.precision();

		System.out.println("Recall = " + recall);
		System.out.println("Precision = " + precision);
	}

	private JavaRDD<Tuple2<Object, Object>> predictLabelOfTestDataFromLinearRegression(JavaRDD<LabeledPoint> test, final LinearRegressionModel linearRegressionModel) {
		return test.map(
					new Function<LabeledPoint, Tuple2<Object, Object>>() {
						public Tuple2<Object, Object> call(LabeledPoint point) {
							double prediction = linearRegressionModel.predict(point.features());
							System.out.println("Prediction: " + prediction);
							return new Tuple2<Object, Object>(prediction, point.label());
						}
					}
			);
	}

	public double computeMeanSquaredError(JavaRDD<Tuple2<Object, Object>> valuesAndPreds) {

		 double MSE = new JavaDoubleRDD(valuesAndPreds.map(
					new Function<Tuple2<Object, Object>, Object>() {
						public Object call(Tuple2<Object, Object> pair) {
							return Math.pow(((double) pair._1()) - ((double) pair._2()), 2.0);
						}
					}
			).rdd()).mean();

		System.out.println("Training Mean Squared Error = " + MSE);

		return MSE;
	}

	private JavaRDD<LabeledPoint>[] splitData(JavaRDD<LabeledPoint> featureLabel, double trainingSplit, double testSplit, long seed) {
		return featureLabel.randomSplit(
				new double[]{trainingSplit, testSplit},
				seed
		);
	}

}

