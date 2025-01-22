package org.gradletraining.loading;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gradletraining.interfaces.Action;

import java.net.URL;

public class CsvLoader implements Loader {

    @Override
    public Dataset<Row> load(SparkSession sparkSession) {
        System.out.println("chargement du DataFrame");
        Dataset<Row> dataset = sparkSession.read()
                .option("header", "true")  // Utiliser la première ligne comme noms de colonnes
                .option("inferSchema", "true")  // Détecter automatiquement les types de données
                .csv(createCsvPath("csv/en.openfoodfacts.org.products.csv"));  // Remplacez par le chemin du fichier
// modifier le séparateur
        return dataset;

    }


    private String createCsvPath(String resourcePath){
        URL resource = getClass().getClassLoader().getResource(resourcePath);

        if (resource == null) {
            throw new IllegalArgumentException("Fichier CSV non trouvé dans resources/csv/ !");
        }

        // Convertir en chemin utilisable par Spark
        String csvPath = resource.getPath().replace("%20", " "); // Corriger les espaces

        return csvPath;
    }


}
