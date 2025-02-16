package org.gradletraining.aggregator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Aggregator {
    Dataset<Row> aggregate(Dataset<Row> dataset);

}
