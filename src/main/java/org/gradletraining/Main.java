package org.gradletraining;

import org.gradletraining.loading.CsvLoader;
import org.gradletraining.spark.SessionHandler;
import org.gradletraining.visualization.CsvDataFrameReader;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello world!");


       var sessionHandler = new SessionHandler();
       var session = sessionHandler.startSession();

        var loader = new CsvLoader();
        var dataset = loader.load(session);

        var reader = new CsvDataFrameReader();
        reader.showData(dataset);

        sessionHandler.closeSession(session);
      //  sessionHandler.execute();
    }
}