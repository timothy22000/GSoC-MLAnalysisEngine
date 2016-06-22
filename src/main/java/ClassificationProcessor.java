import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

public class ClassificationProcessor implements Serializable {
	private int noOfIterations;
	private double stepSize;

	public ClassificationProcessor(int noOfIterations, double stepSize) {
		this.noOfIterations = checkNotNull(noOfIterations);
		this.stepSize = checkNotNull(stepSize);
	}

	public void linearRegressionWithSGD(DataFrame logsAfterKMeans) {
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
		JavaRDD<LabeledPoint>[] splits = splitData(featureLabel, 0.4, 0.6, 11L);
		JavaRDD<LabeledPoint> training = splits[0].cache();
		JavaRDD<LabeledPoint> test = splits[1];

		//More interesting complex analysis with two or more features.

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

		double MSE = new JavaDoubleRDD(valuesAndPreds.map(
				new Function<Tuple2<Object, Object>, Object>() {
					public Object call(Tuple2<Object, Object> pair) {
						return Math.pow(((double) pair._1()) - ((double) pair._2()), 2.0);
					}
				}
		).rdd()).mean();

		System.out.println("Training Mean Squared Error = " + MSE);

		//Evaluation step
		BinaryClassificationMetrics binaryClassificationMetrics = new BinaryClassificationMetrics(valuesAndPreds.rdd(), 0);

		JavaRDD<Tuple2<Object, Object>> roc = binaryClassificationMetrics.roc().toJavaRDD();

		System.out.println("ROC curve: " + roc.toArray());
		System.out.println("Area under ROC curve:" + binaryClassificationMetrics.areaUnderROC());
	}

	private JavaRDD<LabeledPoint>[] splitData(JavaRDD<LabeledPoint> featureLabel, double trainingSplit, double testSplit, long seed) {
		return featureLabel.randomSplit(new double[]{trainingSplit, testSplit}, seed);
	}

}

