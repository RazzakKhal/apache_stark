package org.gradletraining.transformation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class DatasetTransformer implements Transformer {

    private final int MAX_SUGAR_CONTENT = 5;
    private final int MAX_FAT_CONTENT = 3;


    /**
     * transforme le dataset en ajoutant la colonne is_healthy et ingredient_count
     * @param dataset
     * @return
     */
    @Override
    public Dataset<Row> transform(Dataset<Row> dataset){
        return dataset
                .withColumn("is_healthy", functions.when(
                        functions.col("sugars_100g").lt(MAX_SUGAR_CONTENT).and(functions.col("fat_100g").lt(MAX_FAT_CONTENT)),
                        true
                ).otherwise(false))
                .withColumn("ingredient_count", functions.size(functions.split(functions.col("ingredients_text"), ",")));

    }

    @Override
    public Dataset<Row> filter(Dataset<Row> dataset, String column){
        return dataset.filter(functions.col("countries").contains(column.toLowerCase()));
    }

}
