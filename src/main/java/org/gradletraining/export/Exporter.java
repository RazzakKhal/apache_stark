package org.gradletraining.export;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Exporter {

    public void export(Dataset<Row> dataset, String output);
}
