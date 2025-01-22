package org.gradletraining;

import org.gradletraining.loading.CsvLoader;
import org.gradletraining.spark.SessionHandler;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");


       var sessionHandler = new SessionHandler();
       var session = sessionHandler.startSession();

        var loader = new CsvLoader();
        loader.load(session);


        sessionHandler.closeSession(session);
      //  sessionHandler.execute();
    }
}