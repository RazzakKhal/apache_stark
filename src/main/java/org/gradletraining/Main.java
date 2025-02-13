package org.gradletraining;

import org.gradletraining.cleaning.CsvCleaner;
import org.gradletraining.loading.CsvLoader;
import org.gradletraining.spark.SessionHandler;
import org.gradletraining.visualization.CsvDataFrameReader;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        System.out.println("Hello world!");


       var sessionHandler = new SessionHandler();
       var session = sessionHandler.startSession();

       try{

           var loader = new CsvLoader();
           var dataset = loader.load(session);

           var reader = new CsvDataFrameReader();
           reader.showData(dataset);
           //  sessionHandler.execute();

           var cleaner = new CsvCleaner();
           var cleanedDataset = cleaner.clean(dataset);

           reader.showData(cleanedDataset);
       }
       catch (Exception e) {
           System.err.println(e.getMessage());
       }


        sessionHandler.closeSession(session);
    }
}