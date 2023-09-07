/* Copyright OpenSearch Contributors
    * SPDX-License-Identifier: Apache-2.0
    */

package org.opensearch.sql.spark.client;

import com.amazonaws.services.emrserverless.AWSEMRServerless;
import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRun;
import com.amazonaws.services.emrserverless.model.StartJobRunResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.spark.helper.FlintHelper;
import org.opensearch.sql.spark.response.SparkResponse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_APPLICATION_ID;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_EXECUTION_ROLE;
import static org.opensearch.sql.spark.constants.TestConstants.EMRS_JOB_NAME;
import static org.opensearch.sql.spark.constants.TestConstants.QUERY;

@ExtendWith(MockitoExtension.class)
public class EmrServerlessClientImplTest {
  @Mock private AWSEMRServerless emrServerless;
  @Mock private FlintHelper flint;
  @Mock private SparkResponse sparkResponse;

  @Test
  void testStartJobRun() {
    StartJobRunResult response = new StartJobRunResult();
    when(emrServerless.startJobRun(any())).thenReturn(response);

    EmrServerlessClientImpl emrServerlessClient = new EmrServerlessClientImpl(
        emrServerless, EMRS_APPLICATION_ID, EMRS_EXECUTION_ROLE, flint, null, sparkResponse);

    emrServerlessClient.startJobRun(EMRS_JOB_NAME, QUERY);
  }

  @Test
  void testGetJobRunState() {
    JobRun jobRun = new JobRun();
    jobRun.setState("Running");
    GetJobRunResult response = new GetJobRunResult();
    response.setJobRun(jobRun);
    when(emrServerless.getJobRun(any())).thenReturn(response);

    EmrServerlessClientImpl emrServerlessClient = new EmrServerlessClientImpl(
        emrServerless, EMRS_APPLICATION_ID, EMRS_EXECUTION_ROLE, flint, "", sparkResponse);

    emrServerlessClient.getJobRunState("123");
  }

  @Test
  void testCancelJobRun() {
    when(emrServerless.cancelJobRun(any())).thenReturn(new CancelJobRunResult());

    EmrServerlessClientImpl emrServerlessClient = new EmrServerlessClientImpl(
        emrServerless, EMRS_APPLICATION_ID, EMRS_EXECUTION_ROLE, flint, null, sparkResponse);

    emrServerlessClient.cancelJobRun("123");
  }
}
