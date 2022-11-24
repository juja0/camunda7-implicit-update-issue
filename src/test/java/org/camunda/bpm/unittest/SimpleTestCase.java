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

import org.camunda.bpm.engine.impl.persistence.entity.VariableInstanceEntity;
import org.camunda.bpm.engine.runtime.ProcessInstance;
import org.camunda.bpm.engine.test.Deployment;
import org.camunda.bpm.engine.test.junit5.ProcessEngineExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.camunda.bpm.engine.test.assertions.bpmn.BpmnAwareTests.runtimeService;

/**
 * Simple test case to demonstrate redundant updates when checking
 * for implicit variable value modifications in TypedValueField.isValuedImplicitlyUpdated()
 * <p>
 * Note: this has only been observed to happen when the engine's 'defaultSerializationFormat' is set to 'JSON'
 * and when using variables which don't look the same after a round-trip serialization and deserialization.
 * <p>
 * For example: java.time.Instant may get serialized as a double/BigDecimal in json
 * <p>
 * A potential fix has been included in the below file.
 * src/test/java/org/camunda/bpm/engine/impl/persistence/entity/util/TypedValueField.java
 * The contents of the file have been commented out. Uncomment them for the test to pass.
 * Look for comment "potential_fix" for the relevant change that gets the test to pass.
 */
public class SimpleTestCase
{
	private static final String VARIABLE_NAME = "foo";

	@RegisterExtension
	static ProcessEngineExtension extension = ProcessEngineExtension.builder().build();

	@Test
	@Deployment(resources = {"testProcess.bpmn"})
	public void revisionShouldNotBeModifiedWhenRetrievingVariables() throws SQLException
	{
		// start process instance and create a variable named 'foo'
		ProcessInstance processInstance = startInstance("retrieve-test-1");

		// retrieve the "revision" column value from 
		// 'act_ru_variable' for the variable we created above.
		// we query the database directly here to avoid creating a
		// CommandContext. This is because the actual issue happens
		// when the CommandContext is closed and listeners are fired.
		// specifically, in TypedValueField.onCommandContextClose
		assertThat(getRevisionUsingNativeSql()).isEqualTo(1);

		// retrieve the revision again but this time using
		// runtimeService which runs the query inside a CommandContext
		//
		// Ideally, this operation should not modify the database since
		// we are only retrieving the variable and not modifying its value.
		//
		// This is the series of events that leads to the issue.
		//
		// 1. When the variable (a map containing a java.time.Instant)
		//    is created and persisted in db, the serialized value holds a 
		//    BigDecimal representation of the instant and since the map is a 
		//    generic Map<String, Object>, there is nothing to indicate that 
		//    the original value was an Instant. This is fine and is expected, 
		//    since we've registered JavaTimeModule which is supposed to do this.
		// 2. When the variable is later retrieved in a fresh CommandContext,
		//    the value is deserialized and cached in a temporary instance field.
		//    Any updates during the lifetime of the CommandContext are expected
		//    to be reflected in this cached (deserialized) field.
		// 3. When the CommandContext is closed, in order to detect implicit updates,
		//    the cached value is again serialized to a byte array. At this point, 
		//    since we are no longer serializing an Instant, but a BigDecimal or Double
		//    representation of the instant, the serialized value gets converted to
		//    scientific notation which does not match the original serialized representation.
		//    So, even if the variable was not updated at all, there would be a redundant
		//    database update for the variable, consequently incrementing the revision as well.
		//
		// We expect the revision below to be 1 but is actually 2
		int revision = getVariableInstance(processInstance).getRevision();

		// this check fails.
		// expected = 1 but actual = 2
		assertThat(getRevisionUsingNativeSql()).isEqualTo(1);

		// this also fails.
		// expected = 1 but actual = 2
		assertThat(revision).isEqualTo(1);
	}

	@Test
	@Deployment(resources = {"testProcess.bpmn"})
	public void shouldNotResultInOptimisticLockingErrorOnConcurrentExecution()
	{
		// The scenario mentioned above in the previous test would sometimes result in an 
		// OptimisticLockingException when variables are retrieved (not updated) concurrently.
		// 
		// The code statement below does the following.
		//
		// 1. creates a process instance.
		// 2. retrieves the same variable from multiple threads.
		assertThatCode(() -> createProcessAndRetrieveVariable("opt-lock-test-1"))
				.doesNotThrowAnyException();

		// The statement above may not always fail.
		// So we run a few more times to increase the chance of failure.

		IntFunction<Runnable> factory = iteration -> () -> createProcessAndRetrieveVariable("opt-lock-test-" + iteration);

		assertThatCode(() -> executeConcurrently(Executors.newFixedThreadPool(10), 100, factory))
				.doesNotThrowAnyException();
	}

	private static void createProcessAndRetrieveVariable(String businessKey)
	{
		ProcessInstance processInstance = startInstance(businessKey);

		IntFunction<Runnable> factory = (i) -> () -> assertThat(getVariableInstance(processInstance).getName()).isEqualTo(VARIABLE_NAME);

		executeConcurrently(Executors.newCachedThreadPool(), 10, factory);
	}

	private static ProcessInstance startInstance(String businessKey)
	{
		return runtimeService().startProcessInstanceByKey("testProcess", businessKey, createInputVariables());
	}

	private static int getRevisionUsingNativeSql() throws SQLException
	{
		try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:camunda", "sa", "");
		     Statement statement = connection.createStatement();
		     ResultSet rs = statement.executeQuery("select * from act_ru_variable where name_ = '" + VARIABLE_NAME + "'"))
		{
			assertThat(rs.next()).isTrue();
			return rs.getInt("REV_");
		}
	}

	private static VariableInstanceEntity getVariableInstance(ProcessInstance processInstance)
	{
		return ((VariableInstanceEntity) extension.getRuntimeService()
				.createVariableInstanceQuery()
				.processInstanceIdIn(processInstance.getProcessInstanceId())
				.variableName(VARIABLE_NAME)
				.singleResult());
	}

	private static Map<String, Object> createInputVariables()
	{
		Map<String, Object> inputVariables = new HashMap<>();

		inputVariables.put(VARIABLE_NAME, Instant.ofEpochSecond(1234567890).plusNanos(123456789));

		return inputVariables;
	}

	private static void executeConcurrently(ExecutorService pool, int count, IntFunction<Runnable> runnableFactory)
	{
		List<Future<?>> futures = IntStream
				.rangeClosed(1, count)
				.mapToObj(runnableFactory)
				.map(pool::submit)
				.collect(Collectors.toList());

		pool.shutdown();

		futures.forEach(SimpleTestCase::waitForCompletion);
	}

	private static void waitForCompletion(Future<?> future)
	{
		try
		{
			future.get();
		}
		catch (InterruptedException | ExecutionException e)
		{
			throw new RuntimeException(e);
		}
	}

	// https://github.com/juja0/camunda7-implicit-update-issue
	// https://github.com/camunda/camunda-bpm-platform/issues/2110
}
