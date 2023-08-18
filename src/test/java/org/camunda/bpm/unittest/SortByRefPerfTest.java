/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. Camunda licenses this file to you under the Apache License,
 * Version 2.0; you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.bpm.unittest;

import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.camunda.bpm.engine.impl.cfg.ProcessEngineConfigurationImpl;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.test.Deployment;
import org.camunda.bpm.engine.test.junit5.ProcessEngineExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.camunda.bpm.engine.test.assertions.bpmn.BpmnAwareTests.runtimeService;

/**
 * Simple test to demonstrate the impact of the sorting logic in DbOperationManager#sortByReferences
 * <p>
 * On a local test machine, for 1000 instances, the execution times are,
 * <p>
 * With fast sort = ~18 secs
 * Without fast sort = ~82 secs
 */
public class SortByRefPerfTest
{
	private static final int INSTANCES = 1000;
	private static final boolean USE_FAST_SORT = true; // change this to false to use the default sort algorithm

	@RegisterExtension
	static ProcessEngineExtension extension = ProcessEngineExtension.builder().build();

	@Test
	@Deployment(resources = {"testProcess.bpmn"})
	public void testSortPerformance() throws Exception
	{
		ProcessEngineConfigurationImpl configuration = extension.getProcessEngineConfiguration();

		if (USE_FAST_SORT)
		{
			// monkey-patching session factory to demonstrate the issue.
			MethodUtils.invokeMethod(configuration, true, "addSessionFactory", new ExtendedDbEntityManagerFactory(configuration));
		}

		System.out.println("Creating instances");

		List<String> instanceIds = IntStream.rangeClosed(1, INSTANCES)
				.mapToObj(i -> startInstance("retrieve-test-" + i))
				.map(ProcessInstance::getId)
				.collect(Collectors.toList());

		System.out.println("Created instances");

		StopWatch watch = StopWatch.createStarted();

		System.out.println("Deleting instances");

		configuration.getCommandExecutorTxRequiresNew().execute(ctx ->
		{
			runtimeService().deleteProcessInstances(instanceIds, "test", true, true, true);
			return null;
		});

		System.out.println("Deleted instances in " + watch.formatTime());
	}

	private static ProcessInstance startInstance(String businessKey)
	{
		return runtimeService().startProcessInstanceByKey("testProcess", businessKey, createInputVariables());
	}

	private static Map<String, Object> createInputVariables()
	{
		Map<String, Object> inputVariables = new HashMap<>();
		IntStream.rangeClosed(1, 100).forEach(value -> inputVariables.put("foo" + value, "bar" + value));
		return inputVariables;
	}
}
