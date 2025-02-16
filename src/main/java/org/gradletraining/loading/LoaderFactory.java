package org.gradletraining.loading;

public class LoaderFactory {
    public static Loader getLoader(String isLimited) {
        if (isLimited.equalsIgnoreCase("y")) {
            return new LimitedCsvLoader();
        } else {
            return new FullCsvLoader();
        }
    }
}
