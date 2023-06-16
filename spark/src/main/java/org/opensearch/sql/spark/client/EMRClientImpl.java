package org.opensearch.sql.spark.client;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;

public class EMRClientImpl implements SparkClient {

    private static final Logger logger = LogManager.getLogger(EMRClientImpl.class);
    public EMRClientImpl() {

    }

    @Override
    public JSONObject sql(String query) throws IOException {
        AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new ProfileCredentialsProvider("emr-user").getCredentials()))
                .withRegion(Regions.US_WEST_2)
                .build();

        HadoopJarStepConfig stepConfig = new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("spark-submit",
                        "--class","org.opensearch.sql.SQLJob",
                        "--jars","s3://myemrclusterlogs/jar/flint-spark-integration-assembly-0.1.0-SNAPSHOT.jar",
                        "s3://myemrclusterlogs/jar/sql-job.jar",
                        "SELECT * from my_table",
                        ".query_execution_result",
                        "search-test-for-fidelity-fcmtzib4plbqyh4kbkmurk6acu.us-west-2.es.amazonaws.com",
                        "-1",
                        "https",
                        "sigv4",
                        "us-west-2"
                );
        //.withProperties(new KeyValue("spark.emr.stepId", "${emr.step.id}"));

        StepConfig step = new StepConfig()
                .withName("Spark Application Step")
                .withActionOnFailure(ActionOnFailure.CONTINUE)
                .withHadoopJarStep(stepConfig);

        String clusterId = "j-23PYT6MNJB8AY";

        AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
                .withJobFlowId(clusterId)
                .withSteps(step);

        AddJobFlowStepsResult result = emrClient.addJobFlowSteps(request);
        System.out.println("Spark application step submitted successfully. Step IDs: " + result.getStepIds());

        // Create the DescribeStepRequest
        DescribeStepRequest stepRequest = new DescribeStepRequest()
                .withClusterId(clusterId)
                .withStepId(result.getStepIds().get(0));

        // Wait for the step to complete
        boolean completed = false;
        while (!completed) {
            // Get the step status
            DescribeStepResult stepResult = emrClient.describeStep(stepRequest);
            StepStatus statusDetail = stepResult.getStep().getStatus();
            System.out.println("Current status: " + statusDetail.getState());
            // Check if the step has completed
            if (statusDetail.getState().equals("COMPLETED")) {
                completed = true;
                System.out.println("Step completed successfully.");
            } else if (statusDetail.getState().equals("FAILED") || statusDetail.getState().equals("CANCELLED")) {
                completed = true;
                System.out.println("Step failed or cancelled.");
            } else {
                // Sleep for some time before checking the status again
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        // Close the EMR client
        emrClient.shutdown();
        return new JSONObject();
    }
}
