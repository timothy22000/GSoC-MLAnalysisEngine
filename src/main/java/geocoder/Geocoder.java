package geocoder;

import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.model.GeocodingResult;

import java.io.Serializable;

public class Geocoder implements Serializable {
	private final GeoApiContext context;

	public Geocoder(String apiKey) {
		context = new GeoApiContext().setApiKey(apiKey);

	}

	public void geocode(String address) throws Exception {
		GeocodingResult[] results =  GeocodingApi.geocode(context,
				address).await();
		System.out.println("Hello");
		if(results.length != 0) {
			System.out.println(results[0].geometry.location.);
		}
	}
}
