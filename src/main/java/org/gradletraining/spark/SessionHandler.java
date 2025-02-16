package org.gradletraining.spark;

import org.apache.spark.sql.SparkSession;

public class SessionHandler {


    public SparkSession startSession(){
        SparkSession spark = SparkSession.builder()
                .appName("My Spark Application")  // Nom de l’application
                .config("spark.master", "local") // Mode local avec 1 seul nœud
                .getOrCreate();
        System.out.println("SparkSession créé avec succès !");
        System.out.println("Application ID: " + spark.sparkContext().applicationId());

        return spark;

    }

    public void closeSession(SparkSession sparkSession){
        sparkSession.stop();
        System.out.println("Session spark correctement arretée");
    }
}
