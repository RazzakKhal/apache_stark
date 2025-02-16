package org.gradletraining.cleaning;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Cleaner {
    public Dataset<Row> clean(Dataset<Row> dataset);
}
