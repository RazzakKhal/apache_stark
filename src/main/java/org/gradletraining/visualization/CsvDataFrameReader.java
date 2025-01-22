package org.gradletraining.visualization;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CsvDataFrameReader {

    public void showData(Dataset<Row> dataset){
        dataset.show();
        dataset.printSchema();
    }
}
