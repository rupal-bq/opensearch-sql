package org.opensearch.sql.spark.client;

import org.apache.spark.launcher.SparkLauncher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;

public class SparkClientImpl implements SparkClient{
    private static final Logger logger = LogManager.getLogger(SparkClientImpl.class);

    public SparkClientImpl() {

    }

    @Override
    public JSONObject sql(String query) throws IOException {
        try {
            SparkLauncher launcher = new SparkLauncher()
                    .setAppName("MySparkApplication")
                    //.setSparkHome("/Users/maharup/spark") // Set the path to your Spark installation
                    .setAppResource("https://myemrclusterlogs.s3.us-west-2.amazonaws.com/jar/sql-job.jar") // Set the path to your Spark application JAR
                    .setMainClass("org.opensearch.sql.SQLJob") // Set the main class of your Spark application
                    .addAppArgs("select 5",".query_execution_result","localhost","9200","http","false","us-west-2") // Set any application-specific arguments
                    .addJar("https://myemrclusterlogs.s3.us-west-2.amazonaws.com/jar/flint-spark-integration-assembly-0.1.0-SNAPSHOT.jar")
                    .addJar("https://myemrclusterlogs.s3.us-west-2.amazonaws.com/jar/aws-java-sdk-1.12.493.jar")
                    .setMaster("spark://b0f1d879dff7.ant.amazon.com:7077"); // Set the Spark master URL of the remote cluster

            Process process = launcher.launch(); // Launch the Spark application
            int exitCode = process.waitFor(); // Wait for the application to finish

            logger.info("Spark Application return code: "+ exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new JSONObject();
    }
}
