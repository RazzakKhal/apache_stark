package org.gradletraining.loading;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class LimitedCsvLoader implements Loader {
    private final String[] definedColumns = {"product_name", "brands", "countries", "ingredients_text", "labels", "packaging", "carbohydrates_100g", "sugars_100g", "proteins_100g", "salt_100g", "sodium_100g", "fat_100g", "energy_100g", "energy-kcal_100g"};

    @Override
    public Dataset<Row> load(SparkSession sparkSession) throws IOException {
        System.out.println("Chargement du DataFrame limité");
        String limitedCsvPath = createLimitedCsv("csv/en.openfoodfacts.org.products.csv", 1000);
        Dataset<Row> dataset = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", "\t")
                .csv(limitedCsvPath);
        return filterData(dataset, definedColumns);
    }

    private Dataset<Row> filterData(Dataset<Row> dataset, String[] defColumns) {
        var datasetColumns = Arrays.stream(dataset.columns())
                .filter(Arrays.asList(defColumns)::contains)
                .map(column -> "`" + column.replace("`", "") + "`")
                .toArray(String[]::new);
        return dataset.selectExpr(datasetColumns);
    }

    private String createLimitedCsv(String resourcePath, int limit) throws IOException {
        URL resource = getClass().getClassLoader().getResource(resourcePath);
        if (resource == null) throw new IllegalArgumentException("Fichier CSV non trouvé !");

        Path tempFile = Files.createTempFile("limited_csv", ".csv");
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(resource.getPath())));
             BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile.toFile()))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null && count < limit) {
                writer.write(line);
                writer.newLine();
                count++;
            }
        }
        return tempFile.toAbsolutePath().toString();
    }
}
