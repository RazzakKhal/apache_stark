package org.gradletraining.spark;

import org.apache.spark.sql.SparkSession;
import org.gradletraining.interfaces.Action;

public class SessionHandler {

    Action action;

 /*   public SessionHandler(Action action){
        this.action = action;
    }
*/
    public void execute(){
       var session = startSession();
        this.action.executeAction();
        closeSession(session);
    }

    public SparkSession startSession(){
        // Initialiser la SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("My Spark Application")  // Nom de l’application
                .config("spark.master", "local") // Mode local avec 1 seul nœud
                .getOrCreate();

        // Afficher les informations de la session
        System.out.println("SparkSession créé avec succès !");
        System.out.println("Application ID: " + spark.sparkContext().applicationId());

        return spark;

    }

    public void closeSession(SparkSession sparkSession){
        // Arrêter Spark
        sparkSession.stop();
        System.out.println("Session spark correctement arretée");
    }
}
