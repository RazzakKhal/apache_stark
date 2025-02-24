package org.gradletraining.loading;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public interface Loader {

    Dataset<Row> load(SparkSession sparkSession) throws IOException;
}
