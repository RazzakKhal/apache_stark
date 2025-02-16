package org.gradletraining.aggregator;

public class AggregatorFactory {
    public static Aggregator getAggregator(String type) {
        switch (type.toLowerCase()) {
            case "brands":
                return new BrandAggregator();
            case "countries":
                return new CountryAggregator();
            case "labels":
                return new LabelAggregator();
            default:
                throw new IllegalArgumentException("Type d'agrégation non supporté : " + type);
        }
    }
}
