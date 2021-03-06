/*
 * Copyright (c) 2014 Villu Ruusmann
 *
 * This file is part of Openscoring
 *
 * Openscoring is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Openscoring is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Openscoring.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.openscoring.service;

import java.io.*;
import java.util.*;

import javax.xml.transform.*;

import org.openscoring.common.*;

import org.jpmml.evaluator.*;
import org.jpmml.model.*;

import com.google.common.collect.*;

import org.dmg.pmml.*;

import org.junit.*;

import org.supercsv.prefs.*;

import org.xml.sax.*;

import static org.junit.Assert.*;

public class ModelServiceTest {

	@Test
	public void decisionTreeIris() throws Exception {
		ModelService service = createService("DecisionTreeIris");

		List<EvaluationRequest> requests = loadRequest("Iris");
		List<EvaluationResponse> result = service.evaluateBatch("DecisionTreeIris", requests);

		List<EvaluationResponse> responses = loadResponse("DecisionTreeIris");

		compare(responses, result);
	}

	@Test
	public void associationRulesShopping() throws Exception {
		ModelService service = createService("AssociationRulesShopping");

		List<EvaluationRequest> requests = loadRequest("Shopping");
		List<EvaluationResponse> result = service.evaluateBatch("AssociationRulesShopping", requests);

		List<EvaluationResponse> responses = loadResponse("AssociationRulesShopping");

		compare(responses, result);

		List<EvaluationRequest> aggregatedRequests = ModelService.aggregateRequests("transaction", requests);
		List<EvaluationResponse> aggregatedResult = service.evaluateBatch("AssociationRulesShopping", aggregatedRequests);

		assertTrue(aggregatedRequests.size() < requests.size());

		compare(responses, aggregatedResult);
	}

	static
	private void compare(List<EvaluationResponse> expectedResponses, List<EvaluationResponse> actualResponses){
		assertEquals(expectedResponses.size(), actualResponses.size());

		for(int i = 0; i < expectedResponses.size(); i++){
			EvaluationResponse expectedResponse = expectedResponses.get(i);
			EvaluationResponse actualResponse = actualResponses.get(i);

			compare(expectedResponse.getResult(), actualResponse.getResult());
		}
	}

	static
	private void compare(Map<String, ?> expectedResult, Map<String, ?> actualResult){
		assertEquals(expectedResult.size(), actualResult.size());

		Set<String> keys = expectedResult.keySet();
		for(String key : keys){
			String expectedValue = (String)expectedResult.get(key);
			Object actualValue = actualResult.get(key);

			if(actualValue instanceof Collection){

				if(expectedValue.startsWith("[") && expectedValue.endsWith("]")){
					expectedValue = expectedValue.substring(1, expectedValue.length() - 1);

					String[] expectedElements = (expectedValue.length() > 0 ? expectedValue.split(",\\s?") : new String[0]);

					assertTrue(acceptable(Arrays.asList(expectedElements), (List<?>)actualValue));
				} else

				{
					fail();
				}
			} else

			{
				assertTrue(acceptable(expectedValue, actualValue));
			}
		}
	}

	static
	private boolean acceptable(List<String> expectedValues, List<?> actualValues){

		if(expectedValues.size() != actualValues.size()){
			return false;
		}

		boolean result = true;

		for(int i = 0; i < expectedValues.size(); i++){
			String expectedValue = expectedValues.get(i);
			Object actualValue = actualValues.get(i);

			result &= acceptable(expectedValue, actualValue);
		}

		return result;
	}

	static
	private boolean acceptable(String expectedValue, Object actualValue){
		return VerificationUtil.acceptable(TypeUtil.parse(TypeUtil.getDataType(actualValue), expectedValue), actualValue, ModelServiceTest.precision, ModelServiceTest.zeroThreshold);
	}

	static
	private ModelService createService(String id) throws Exception {
		ModelRegistry registry = new ModelRegistry();
		registry.put(id, loadPMML(id));

		return new ModelService(registry);
	}

	static
	private PMML loadPMML(String id) throws Exception {
		InputStream is = ModelServiceTest.class.getResourceAsStream("/pmml/" + id + ".pmml");

		try {
			Source source = ImportFilter.apply(new InputSource(is));

			return JAXBUtil.unmarshalPMML(source);
		} finally {
			is.close();
		}
	}

	static
	private List<EvaluationRequest> loadRequest(String id) throws Exception {
		InputStream is = ModelServiceTest.class.getResourceAsStream("/csv/" + id + ".csv");

		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));

			try {
				return CsvUtil.readTable(reader, CsvPreference.TAB_PREFERENCE, "Id");
			} finally {
				reader.close();
			}
		} finally {
			is.close();
		}
	}

	static
	private List<EvaluationResponse> loadResponse(String id) throws Exception {
		return convert(loadRequest(id));
	}

	static
	private List<EvaluationResponse> convert(List<EvaluationRequest> requests){
		List<EvaluationResponse> responses = Lists.newArrayList();

		for(EvaluationRequest request : requests){
			EvaluationResponse response = new EvaluationResponse(request.getId());
			response.setResult(request.getArguments());

			responses.add(response);
		}

		return responses;
	}

	private static final double precision = 1d / (1000 * 1000);

	private static final double zeroThreshold = precision;
}