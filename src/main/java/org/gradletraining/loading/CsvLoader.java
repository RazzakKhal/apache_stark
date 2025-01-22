package org.gradletraining.loading;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.gradletraining.interfaces.Action;

public class CsvLoader implements Loader {

    @Override
    public void load(SparkSession sparkSession) {

      Dataset<Row> dataset = sparkSession.read()
                .option("header", "true")  // Utiliser la première ligne comme noms de colonnes
                .option("inferSchema", "true")  // Détecter automatiquement les types de données
                .csv("C:/Users/Pc/Desktop/TP_integration_donnee/csv/en.openfoodfacts.org.products.csv");  // Remplacez par le chemin du fichier


        System.out.println("chargement du DataFrame");
    }


}
