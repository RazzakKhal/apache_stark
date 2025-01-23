package org.gradletraining.loading;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gradletraining.interfaces.Action;

import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Scanner;

public class CsvLoader implements Loader {

    private final String[] columns = {"product_name", "brands", "countries", "ingredients_text", "labels", "packaging"};
    private final String resourcePath = "csv/en.openfoodfacts.org.products.csv";

    @Override
    public Dataset<Row> load(SparkSession sparkSession) throws IOException {

        System.out.println("Entre y pour travailler sur le fichier limité");
        var limited = new Scanner(System.in);
        var resp = limited.next();
        limited.close();

        String limitedCsvPath = createLimitedCsv("csv/en.openfoodfacts.org.products.csv", 1000);
        Dataset<Row> dataset;

        if(resp.equals("y")){
            System.out.println("chargement du DataFrame limité");
            dataset = sparkSession.read()
                    .option("header", "true")  // Utiliser la première ligne comme noms de colonnes
                    .option("inferSchema", "true")  // Détecter automatiquement les types de données
                    .option("delimiter", "\t")  // Définir le séparateur comme une tabulation
                    .csv(limitedCsvPath);  // Remplacez par le chemin du fichier
        }else{
            System.out.println("chargement du DataFrame complet");
            dataset = sparkSession.read()
                    .option("header", "true")  // Utiliser la première ligne comme noms de colonnes
                    .option("inferSchema", "true")  // Détecter automatiquement les types de données
                    .option("delimiter", "\t")  // Définir le séparateur comme une tabulation
                    .csv(getClass().getClassLoader().getResource(resourcePath).getPath());  // Remplacez par le chemin du fichier
        }




// modifier le séparateur
        return filterData(dataset, columns);

    }

    private Dataset<Row> filterData(Dataset<Row> dataset, String[] columns){
        return dataset.selectExpr(columns);
    }


    private String createLimitedCsv(String resourcePath, int limit) throws IOException {
        URL resource = getClass().getClassLoader().getResource(resourcePath);
        if (resource == null) {
            throw new IllegalArgumentException("Fichier CSV non trouvé dans resources/csv/ !");
        }

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
