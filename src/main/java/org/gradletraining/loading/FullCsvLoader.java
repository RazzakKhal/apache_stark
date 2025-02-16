package org.gradletraining.loading;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Arrays;

public class FullCsvLoader implements Loader {
    private final String[] definedColumns = {"product_name", "brands", "countries", "ingredients_text", "labels", "packaging", "carbohydrates_100g", "sugars_100g", "proteins_100g", "salt_100g", "sodium_100g", "fat_100g", "energy_100g", "energy-kcal_100g"};
    private final String resourcePath = "csv/en.openfoodfacts.org.products.csv";

    @Override
    public Dataset<Row> load(SparkSession sparkSession) throws IOException {
        System.out.println("Chargement du DataFrame complet");
        Dataset<Row> dataset = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", "\t")
                .csv(getClass().getClassLoader().getResource(resourcePath).getPath());
        return filterData(dataset, definedColumns);
    }

    private Dataset<Row> filterData(Dataset<Row> dataset, String[] defColumns) {
        var datasetColumns = Arrays.stream(dataset.columns())
                .filter(Arrays.asList(defColumns)::contains)
                .map(column -> "`" + column.replace("`", "") + "`")
                .toArray(String[]::new);
        return dataset.selectExpr(datasetColumns);
    }
}
