package converter;

import java.util.List;
import java.util.Map;

public class JsonToCsvConverter {
	private JsonProcessor jsonProcessor;
	private CsvOutputWriter csvOutputWriter;

	public JsonToCsvConverter(JsonProcessor jsonProcessor, CsvOutputWriter csvOutputWriter) {
		this.jsonProcessor = jsonProcessor;
		this.csvOutputWriter = csvOutputWriter;
	}

	public String createCsvFile(String jsonString) throws Exception {
		List<Map<String, String>> flatJson =jsonProcessor.parseJson(jsonString);

		String fileName = "test.csv";
		csvOutputWriter.writeAsCSV(flatJson, fileName);

		return fileName;
	}

	public JsonProcessor getJsonProcessor() {
		return jsonProcessor;
	}

	public CsvOutputWriter getCsvOutputWriter() {
		return csvOutputWriter;
	}
}
