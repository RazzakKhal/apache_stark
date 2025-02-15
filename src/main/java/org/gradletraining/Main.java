package org.gradletraining;

import org.gradletraining.cleaning.CsvCleaner;
import org.gradletraining.export.CsvExporter;
import org.gradletraining.loading.CsvLoader;
import org.gradletraining.spark.SessionHandler;
import org.gradletraining.transformation.DatasetTransformerImpl;
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

           var transformer = new DatasetTransformerImpl();
           var transformedData = transformer.transform(cleanedDataset);
           var filteredData = transformer.filter(transformedData, "france");

           System.out.println("Les data transformées");
           reader.showData(filteredData);

           var exporter = new CsvExporter();
           exporter.export(filteredData, "C:\\Users\\Pc\\Desktop\\TP_integration_donnee\\src\\main\\java\\org\\gradletraining\\filteredData");
       }
       catch (Exception e) {
           System.err.println(e.getMessage());
       }


        sessionHandler.closeSession(session);
    }
}