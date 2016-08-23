package geocoder;

import com.google.common.base.Optional;
import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.GeocodingApiRequest;
import com.google.maps.PendingResult;
import com.google.maps.model.GeocodingResult;
import com.google.maps.model.LatLng;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class Geocoder implements Serializable {
	private static final Logger logger = LoggerFactory.getLogger(Geocoder.class);
	private final GeoApiContext context;

	public Geocoder(String apiKey) {
		context = new GeoApiContext().setApiKey(apiKey);


	}

	public Optional<LatLng> geocode(String address) throws Exception {
		GeocodingApiRequest geocodingApiRequest = GeocodingApi.newRequest(context);
		GeocodingResult[] results = geocodingApiRequest.address(address).await();

		return Optional.fromNullable(results[0].geometry.location);

	}
}
