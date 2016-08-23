package rulegenerator;

import com.clearspring.analytics.util.Lists;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class RuleGenerator {
	private static final Logger logger = LoggerFactory.getLogger(RuleGenerator.class);
	private static final String outputFileDir = "src/main/resources/output";

	private static Comparator<List<String>> comp = new Comparator<List<String>>() {
		public int compare(List<String> csvLine1, List<String> csvLine2) {
			//Get last field which would be the metric.
			return Double.valueOf(csvLine1.get(csvLine1.size() - 1)).compareTo(Double.valueOf(csvLine2.get(csvLine2.size() - 1)));
		}
	};

	public RuleGenerator() {
	}

	public String generateRuleFile(String fileName) {

		File csvFile = new File(outputFileDir + "/" + fileName);
		List<List<String>> csvLinesRulesSplit = Lists.newArrayList();
		String ruleFileName = fileName + "--rule";
		File newRuleFile = new File(outputFileDir + "/" + ruleFileName);
		try {
			List<String> csvLines = FileUtils.readLines(csvFile);

			for(String csvLine : csvLines) {
				String[] csvLineSplit = csvLine.split(",");
				List<String> csvLineRule = Arrays.asList(csvLineSplit);
				csvLinesRulesSplit.add(csvLineRule);
			}

			Collections.sort(csvLinesRulesSplit.subList(1, csvLinesRulesSplit.size()), comp);
			List<String> header = csvLinesRulesSplit.get(0);

			for(List<String> csvRuleLine : csvLinesRulesSplit.subList(1, csvLinesRulesSplit.size())) {
				String rule = "IF ";

				for(int i = 0; i < csvRuleLine.size(); i++) {

					if(i == csvRuleLine.size() - 2) {
						rule += " THEN " + header.get(i) + "=" + csvRuleLine.get(i) + " -- ";
					} else if(i == csvRuleLine.size() - 1) {
						rule += header.get(i) + "=" + csvRuleLine.get(i);
					} else {
						rule += header.get(i) + "=" + csvRuleLine.get(i);
					}

					//Don't add AND for consequent and the evaluation metric
					if(i < header.size() - 2) {
						rule += " AND ";
					}
				}

				rule += "\n";
				String filterRule = rule.replace("AND  THEN", "THEN");
				FileUtils.writeStringToFile(newRuleFile, filterRule, true);
			}

		} catch (java.io.IOException e) {
			logger.error("CSV File was not found for rule generation.");
		}

		System.out.println("Rule File created.");
		return ruleFileName;
	}

}

