/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.data.constants;

public class SparkFieldConstants {
  public static final String FLINT_INTEGRATION_JAR = "s3://myemrclusterlogs/jar/flint-spark-integration-assembly-0.1.0-SNAPSHOT.jar";
  public static final String SPARK_APPLICATION_JAR = "s3://myemrclusterlogs/jar/sql-job.jar";
  public static final String SPARK_INDEX_NAME = ".query_execution_result";

  public static final String INTEGRATION_JAR = "https://myemrclusterlogs.s3.us-west-2.amazonaws.com/jar/flint-spark-integration-assembly-0.1.0-SNAPSHOT.jar";
  public static final String APPLICATION_JAR = "https://myemrclusterlogs.s3.us-west-2.amazonaws.com/jar/sql-job.jar";

  public static final String AWS_JAVA_SDK_JAR = "https://myemrclusterlogs.s3.us-west-2.amazonaws.com/jar/aws-java-sdk-1.12.493.jar";
  public static final String STEP_ID_FIELD = "stepId.keyword";
  public static final String APPLICATION_ID_FIELD = "applicationId.keyword";

  public  static final String EMR = "emr";
  public static final String LIVY = "livy";
 }
