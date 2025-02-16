package org.gradletraining;

import org.gradletraining.aggregator.AggregatorFactory;
import org.gradletraining.cleaning.Cleaner;
import org.gradletraining.cleaning.CsvCleaner;
import org.gradletraining.export.CsvExporter;
import org.gradletraining.loading.Loader;
import org.gradletraining.loading.LoaderFactory;
import org.gradletraining.spark.SessionHandler;
import org.gradletraining.transformation.DatasetTransformer;
import org.gradletraining.transformation.Transformer;
import org.gradletraining.visualization.CsvDataFrameReader;
import org.gradletraining.visualization.Reader;

import java.util.Scanner;


public class Main {
    public static void main(String[] args) {

       var sessionHandler = new SessionHandler();
       var session = sessionHandler.startSession();

       try{

           System.out.println("Veux tu utilisé le fichier en version limité (y/n)");
           var limited = new Scanner(System.in);
           var resp = limited.next();
           limited.close();
           Loader loader = LoaderFactory.getLoader(resp);
           var dataset = loader.load(session);

           Reader reader = new CsvDataFrameReader();
           reader.showData(dataset);

           Cleaner cleaner = new CsvCleaner();
           var cleanedDataset = cleaner.clean(dataset);

           reader.showData(cleanedDataset);

           Transformer transformer = new DatasetTransformer();
           var transformedData = transformer.transform(cleanedDataset);
           var filteredData = transformer.filter(transformedData, "france");

           reader.showData(filteredData);


           var brandAggregator = AggregatorFactory.getAggregator("brands");
           var countryAggregator = AggregatorFactory.getAggregator("countries");
           var labelAggregator = AggregatorFactory.getAggregator("labels");

           var topBrands = brandAggregator.aggregate(filteredData);
           var avgSugarEnergyByCountry = countryAggregator.aggregate(filteredData);
           var productDistributionByLabels = labelAggregator.aggregate(filteredData);


           var exporter = new CsvExporter();
           exporter.export(filteredData, "C:\\Users\\Pc\\Desktop\\TP_integration_donnee\\src\\main\\java\\org\\gradletraining\\filteredData");
           exporter.export(topBrands, "C:\\Users\\Pc\\Desktop\\TP_integration_donnee\\src\\main\\java\\org\\gradletraining\\topBrands");
           exporter.export(avgSugarEnergyByCountry, "C:\\Users\\Pc\\Desktop\\TP_integration_donnee\\src\\main\\java\\org\\gradletraining\\avgSugarEnergyByCountry");
           exporter.export(productDistributionByLabels, "C:\\Users\\Pc\\Desktop\\TP_integration_donnee\\src\\main\\java\\org\\gradletraining\\productDistributionByLabels");


       }
       catch (Exception e) {
           System.err.println(e.getMessage());
       }


        sessionHandler.closeSession(session);
    }
}