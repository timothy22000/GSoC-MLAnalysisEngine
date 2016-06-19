import org.apache.spark.mllib.clustering.StreamingKMeans;

//Placeholder for code to be moved here.
public class KafkaSparkStreamingExample {

	public void run(String[] args) {
		/**
		 * Attempt to convert DStream of K,V where V is the JSON into DataFrame to
		 * convert catagorical to numerical data and then changing each DataFrame into Vectors
		 * to have DStream<Vector> which is the argument expected by StreamingKMeans but
		 * couldn't get it to work.
		 *
		 */

		//	    messages.map(new Function<Tuple2<String,String>, Vector>() {
//
//		    @Override
//		    public Vector call(Tuple2<String, String> stringStringTuple2) throws Exception {
//			    System.out.println(stringStringTuple2.toString());
//
//				//Trying to convert JSON to SQL Dataframe for each RDD and then converting them to Vectors
//			    //so we have RDD of Vector which is the input expected for StreamingKMeans
//			    return Vectors.dense(1,1);
//		    }
//	    }).print();

		/**
		 * Streaming KMeans model building
		 */

		int numDimensions = 2;
		int numClusters = 2;

		StreamingKMeans streamingKMeans= new StreamingKMeans()
				.setK(numClusters)
				.setDecayFactor(1.0)
				.setRandomCenters(numDimensions, 0.0, 11L);

//	    streamingKMeans.trainOn();

		System.out.println(streamingKMeans.latestModel().clusterCenters());

	}
}
