package org.gradletraining.visualization;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Reader {
    public void showData(Dataset<Row> dataset);
}
