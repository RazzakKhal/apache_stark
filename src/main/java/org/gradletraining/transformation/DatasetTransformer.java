package org.gradletraining.transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DatasetTransformer {

    public Dataset<Row> transform(Dataset<Row> dataset);

    public Dataset<Row> filter(Dataset<Row> dataset, String column);
}
