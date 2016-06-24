import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;

import static com.google.common.base.Preconditions.checkNotNull;

public class ClusteringProcessor {

	private int noOfClusters;
	private String featureColumnName;
	private String predictionColumnName;
	private KMeans kmeans;
	private KMeansModel kmeansModel;
	private DataFrame clusterResults;

	public ClusteringProcessor(int noOfClusters, String featureColumnName, String predictionColumnName) {
		this.noOfClusters = checkNotNull(noOfClusters);
		this.featureColumnName = checkNotNull(featureColumnName);
		this.predictionColumnName = checkNotNull(predictionColumnName);

		kmeans = new org.apache.spark.ml.clustering.KMeans()
				.setK(noOfClusters)
				.setFeaturesCol(featureColumnName)
				.setPredictionCol(predictionColumnName);
	}

	public KMeansModel startKMeans(DataFrame logsWithFeatures) {


		kmeansModel = kmeans.fit(logsWithFeatures);
		DataFrame logsAfterKMeans = kmeansModel.transform(logsWithFeatures);

		clusterResults = logsAfterKMeans;

		return kmeansModel;
	}

	public void showClusterCentreResult() {
		for (Vector centre : kmeansModel.clusterCenters()) {
			System.out.println(centre);
		}
	}

	public int getNoOfClusters() {
		return noOfClusters;
	}

	public String getFeatureColumnName() {
		return featureColumnName;
	}

	public String getPredictionColumnName() {
		return predictionColumnName;
	}

	public KMeans getKmeans() {
		return kmeans;
	}

	public DataFrame getClusterResults() {
		return clusterResults;
	}
}
