/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMR_CLUSTER_ID;
import static org.opensearch.sql.spark.constants.TestConstants.QUERY;
import static org.opensearch.sql.spark.utils.TestUtils.getJson;

import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.Step;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.helper.EMRHelper;
import org.opensearch.sql.spark.helper.FlintHelper;
import org.opensearch.sql.spark.response.SparkResponse;

@ExtendWith(MockitoExtension.class)
public class EmrClientImplTest {

  @Mock
  private EMRHelper emr;
  @Mock
  private FlintHelper flint;
  @Mock
  private Client client;
  @Mock
  private SparkResponse sparkResponse;
  @Mock
  private EmrClientImpl emrClient;

  @Test
  @SneakyThrows
  void testRunEmrApplication() {
    AddJobFlowStepsResult addStepsResult = new AddJobFlowStepsResult().withStepIds(EMR_CLUSTER_ID);
    when(emr.addStep(any())).thenReturn(addStepsResult);

    StepStatus stepStatus = new StepStatus();
    stepStatus.setState("COMPLETED");
    Step step = new Step();
    step.setStatus(stepStatus);
    DescribeStepResult describeStepResult = new DescribeStepResult();
    describeStepResult.setStep(step);
    when(emr.getStepStatus(any())).thenReturn(stepStatus);

    EmrClientImpl emrClientImpl = new EmrClientImpl(emr, flint, sparkResponse);
    emrClientImpl.runEmrApplication(QUERY);
  }

  @Test
  @SneakyThrows
  void testRunEmrApplicationFailed() {
    AddJobFlowStepsResult addStepsResult = new AddJobFlowStepsResult().withStepIds(EMR_CLUSTER_ID);
    when(emr.addStep(any())).thenReturn(addStepsResult);

    StepStatus stepStatus = new StepStatus();
    stepStatus.setState("FAILED");
    Step step = new Step();
    step.setStatus(stepStatus);
    DescribeStepResult describeStepResult = new DescribeStepResult();
    describeStepResult.setStep(step);
    when(emr.getStepStatus(any())).thenReturn(stepStatus);

    EmrClientImpl emrClientImpl = new EmrClientImpl(emr, flint, sparkResponse);
    RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
        () -> emrClientImpl.runEmrApplication(QUERY));
    Assertions.assertEquals("Spark SQL application failed.",
        exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testRunEmrApplicationCancelled() {
    AddJobFlowStepsResult addStepsResult = new AddJobFlowStepsResult().withStepIds(EMR_CLUSTER_ID);
    when(emr.addStep(any())).thenReturn(addStepsResult);

    StepStatus stepStatus = new StepStatus();
    stepStatus.setState("CANCELLED");
    Step step = new Step();
    step.setStatus(stepStatus);
    DescribeStepResult describeStepResult = new DescribeStepResult();
    describeStepResult.setStep(step);
    when(emr.getStepStatus(any())).thenReturn(stepStatus);

    EmrClientImpl emrClientImpl = new EmrClientImpl(emr, flint, sparkResponse);
    RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
        () -> emrClientImpl.runEmrApplication(QUERY));
    Assertions.assertEquals("Spark SQL application failed.",
        exception.getMessage());
  }

  @Test
  @SneakyThrows
  void testRunEmrApplicationRunnning() {
    AddJobFlowStepsResult addStepsResult = new AddJobFlowStepsResult().withStepIds(EMR_CLUSTER_ID);
    when(emr.addStep(any())).thenReturn(addStepsResult);

    StepStatus runningStatus = new StepStatus();
    runningStatus.setState("RUNNING");
    Step runningStep = new Step();
    runningStep.setStatus(runningStatus);

    StepStatus completedStatus = new StepStatus();
    completedStatus.setState("COMPLETED");
    Step completedStep = new Step();
    completedStep.setStatus(completedStatus);

    DescribeStepResult describeStepResult = new DescribeStepResult();
    describeStepResult.setStep(runningStep);
    when(emr.getStepStatus(any())).thenReturn(runningStatus).thenReturn(completedStatus);

    EmrClientImpl emrClientImpl = new EmrClientImpl(emr, flint, sparkResponse);
    emrClientImpl.runEmrApplication(QUERY);
  }

  @Test
  @SneakyThrows
  void testSql() {
    AddJobFlowStepsResult addStepsResult = new AddJobFlowStepsResult().withStepIds(EMR_CLUSTER_ID);
    when(emr.addStep(any())).thenReturn(addStepsResult);

    StepStatus runningStatus = new StepStatus();
    runningStatus.setState("RUNNING");
    Step runningStep = new Step();
    runningStep.setStatus(runningStatus);

    StepStatus completedStatus = new StepStatus();
    completedStatus.setState("COMPLETED");
    Step completedStep = new Step();
    completedStep.setStatus(completedStatus);

    DescribeStepResult describeStepResult = new DescribeStepResult();
    describeStepResult.setStep(runningStep);
    when(emr.getStepStatus(any())).thenReturn(runningStatus).thenReturn(completedStatus);

    when(sparkResponse.getResultFromOpensearchIndex()).thenReturn(new JSONObject(getJson("select_query_response.json")));

    EmrClientImpl emrClientImpl = new EmrClientImpl(emr, flint, sparkResponse);
    emrClientImpl.sql(QUERY);

  }
}
