package org.gradletraining.aggregator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class CountryAggregator implements Aggregator {
    @Override
    public Dataset<Row> aggregate(Dataset<Row> dataset) {
        return dataset.groupBy("countries")
                .agg(functions.avg("sugars_100g").alias("avg_sugar"),
                        functions.avg("energy_100g").alias("avg_energy"));
    }
}
