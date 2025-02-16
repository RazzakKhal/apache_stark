package org.gradletraining.cleaning;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * responsable du nettoyage des données
 */
public class CsvCleaner implements Cleaner{

    private final String[] textColumns = {
            "product_name", "brands", "countries", "ingredients_text", "labels", "packaging"
    };

    private final String[] numberColumns = {
            "carbohydrates_100g", "sugars_100g", "proteins_100g", "salt_100g", "sodium_100g", "fat_100g", "energy_100g", "energy-kcal_100g"
    };

    /**
     * néttoie un dataset
     */
    public Dataset<Row> clean(Dataset<Row> dataset){
       return cleanDuplicatesValues(standardizeValuesName(deleteUnusableRows(dataset)));
    }


    /**
     * suppression des lignes inutilisables
     */
    private Dataset<Row> deleteUnusableRows(Dataset<Row> dataset){
        return dataset.filter(dataset.col("product_name").isNotNull()
                .and(dataset.col("carbohydrates_100g").isNotNull())
                .and(dataset.col("sugars_100g").isNotNull())
                .and(dataset.col("proteins_100g").isNotNull())
                .and(dataset.col("salt_100g").isNotNull())
                .and(dataset.col("sodium_100g").isNotNull())
                .and(dataset.col("fat_100g").isNotNull())
                .and(dataset.col("energy_100g").isNotNull())
                .and(dataset.col("energy-kcal_100g").isNotNull())
        );
    }


    /**
     * supprime les doublons
     */
    private Dataset<Row> cleanDuplicatesValues(Dataset<Row> dataset){
        return dataset.dropDuplicates();

    }

    /**
     * Uniformise les valeurs des colonnes critiques
     */
    private Dataset<Row> standardizeValuesName(Dataset<Row> dataset){
        for (String column : textColumns) {
            dataset = dataset.withColumn(column, functions.lower(functions.trim(dataset.col(column))));
        }
        return dataset;
    }
}
