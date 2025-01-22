package org.gradletraining.loading;

import org.apache.spark.sql.SparkSession;

public interface Loader {

    void load(SparkSession sparkSession);
}
