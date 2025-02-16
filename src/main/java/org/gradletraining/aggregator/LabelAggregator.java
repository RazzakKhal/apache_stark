package org.gradletraining.aggregator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class LabelAggregator implements Aggregator {
    @Override
    public Dataset<Row> aggregate(Dataset<Row> dataset) {
        return dataset.groupBy("labels").count();
    }
}

