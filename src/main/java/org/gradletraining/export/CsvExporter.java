package org.gradletraining.export;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class CsvExporter implements Exporter{
    @Override
    public void export(Dataset<Row> dataset, String output) {
        dataset.coalesce(1)
                .write()
                .format("csv")
                .option("header", "true")
                .option("delimiter", ";")
                .mode("overwrite").save(output);
    }
}
