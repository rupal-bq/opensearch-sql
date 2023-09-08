/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import com.amazonaws.services.emrserverless.AWSEMRServerless;
import java.io.IOException;

import com.amazonaws.services.emrserverless.model.CancelJobRunRequest;
import com.amazonaws.services.emrserverless.model.GetJobRunRequest;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobDriver;
import com.amazonaws.services.emrserverless.model.SparkSubmit;
import com.amazonaws.services.emrserverless.model.StartJobRunRequest;
import com.amazonaws.services.emrserverless.model.StartJobRunResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.sql.spark.helper.FlintHelper;
import org.opensearch.sql.spark.response.SparkResponse;

import static org.opensearch.sql.spark.data.constants.SparkConstants.SPARK_SQL_APPLICATION_JAR;

public class EmrServerlessClientImpl implements SparkClient {

  private final AWSEMRServerless emrServerless;
  private final String applicationId;
  private final String executionRoleArn;
  private final FlintHelper flint;
  private final String sparkApplicationJar;
  private SparkResponse sparkResponse;
  private static final Logger logger = LogManager.getLogger(EmrServerlessClientImpl.class);

  public EmrServerlessClientImpl(
      AWSEMRServerless emrServerless, String applicationId,
      String executionRoleArn, FlintHelper flint,
      String sparkApplicationJar, SparkResponse sparkResponse) {
    this.emrServerless = emrServerless;
    this.applicationId = applicationId;
    this.executionRoleArn = executionRoleArn;
    this.flint = flint;
    this.sparkApplicationJar = sparkApplicationJar == null ? SPARK_SQL_APPLICATION_JAR : sparkApplicationJar;
    this.sparkResponse = sparkResponse;
  }


  @Override
  public JSONObject sql(String query) throws IOException {
    // TODO: update/ remove for async approach
    String jobId = startJobRun("temp", query);
    sparkResponse.setValue(jobId);
    getJobRunState(jobId);
    return sparkResponse.getResultFromOpensearchIndex();
  }

  public String startJobRun(String jobName, String query) {
    StartJobRunRequest request =
        new StartJobRunRequest()
            .withName(jobName)
            .withApplicationId(applicationId)
            .withExecutionRoleArn(executionRoleArn)
            .withJobDriver(new JobDriver()
                .withSparkSubmit(
                    new SparkSubmit()
                        .withEntryPoint(sparkApplicationJar)
                        .withEntryPointArguments(query)
                        .withSparkSubmitParameters(
                            "--class org.opensearch.sql.SQLJob" +
                                " --conf spark.driver.cores=1" +
                                " --conf spark.driver.memory=1g" +
                                " --conf spark.executor.cores=2" +
                                " --conf spark.executor.memory=4g" +
                                " --conf spark.jars=" + flint.getFlintIntegrationJar() +
                                " --conf spark.datasource.flint.host=" + flint.getFlintHost() +
                                " --conf spark.datasource.flint.port=" + flint.getFlintPort() +
                                " --conf spark.datasource.flint.scheme=" + flint.getFlintScheme() +
                                " --conf spark.datasource.flint.auth=" + flint.getFlintAuth() +
                                " --conf spark.datasource.flint.region=" + flint.getFlintRegion() +
                                " --conf spark.emr-serverless.driverEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64/" +
                                " --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-amazon-corretto.x86_64/" +
                                " --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")));
    StartJobRunResult response = emrServerless.startJobRun(request);
    logger.info("Job Run ID: "+response.getJobRunId());
    return response.getJobRunId();
  }

  public String getJobRunState(String jobRunId) {
    GetJobRunRequest request = new GetJobRunRequest().withApplicationId(applicationId).withJobRunId(jobRunId);
    GetJobRunResult response = emrServerless.getJobRun(request);
    logger.info("Job Run state: "+response.getJobRun().getState());
    return response.getJobRun().getState();
  }

  public void cancelJobRun(String jobRunId) {
    // Cancel the job run
    emrServerless.cancelJobRun(
        new CancelJobRunRequest()
            .withApplicationId(applicationId)
            .withJobRunId(jobRunId));
  }
}
