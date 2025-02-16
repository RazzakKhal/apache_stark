package org.gradletraining.aggregator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class BrandAggregator implements Aggregator {
    @Override
    public Dataset<Row> aggregate(Dataset<Row> dataset) {
        return dataset.groupBy("brands")
                .count()
                .orderBy(functions.desc("count"))
                .limit(10);
    }
}
