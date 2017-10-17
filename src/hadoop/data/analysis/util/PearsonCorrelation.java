package hadoop.data.analysis.util;

import java.util.LinkedList;
import java.util.TreeMap;

public class PearsonCorrelation {

    private double listAAverage;
    private double listBAverage;
    private double aBCorrelation;

    public PearsonCorrelation() {

    }

    public String getCorrelations(LinkedList<Double> listA, String variableA,
                                  LinkedList<Double> listB, String variableB) {
        StringBuilder correlations = new StringBuilder();

        listAAverage = averageForAList(listA);
        listBAverage = averageForAList(listB);

        aBCorrelation = correlationFromList(listA, listAAverage, listB, listBAverage);

        correlations.append(variableA).append(":").append(variableB)
                .append(" correlation: ").append(aBCorrelation).append("\n");

        return correlations.toString();
    }

    private void convertStringToDoubleList(LinkedList<String> originList, LinkedList<Double> destList) {
        try {
            for (String element : originList) {
                destList.add(Double.parseDouble(element));
            }
        } catch (NumberFormatException ignored) {

        }
    }

    private double averageForAList(LinkedList<Double> doubleList) {
        int totalElements = doubleList.size();
        double totalCount = 0.0;
        for (Double dataPoint : doubleList) {
            totalCount += dataPoint;
        }

        return (totalCount/totalElements);
    }

    private double correlationFromList(LinkedList<Double> originList, double originMean,
                                       LinkedList<Double> destList, double destMean) {
        TreeMap<Integer, Double> subtractedAMap = new TreeMap<>();
        TreeMap<Integer, Double> subtractedBMap = new TreeMap<>();

        TreeMap<Integer, Double> intermediateAMap = new TreeMap<>();

        int i = 0;
        for (Double element : originList) {
            double subtractedValue = element - originMean;
            subtractedAMap.put(i, subtractedValue);
            intermediateAMap.put(i, element);
            i++;
        }

        int j = 0;
        for (Double destDouble : destList) {
            double subtracted = destDouble - destMean;
            subtractedBMap.put(j, subtracted);
            j++;
        }

        double totalSquareA = 0.0;
        for (Double subtractedValue : subtractedAMap.values()) {
            totalSquareA += (subtractedValue * subtractedValue);
        }

        double totalSquareB = 0.0;
        for (Double subValue : subtractedBMap.values()) {
            totalSquareB += (subValue * subValue);
        }

        double totalATimesB = 0.0;
        for (Integer index : intermediateAMap.keySet()) {
            try {
                double a = subtractedAMap.get(index);
                double b = subtractedBMap.get(index);

                totalATimesB += (a * b);
            } catch (NullPointerException ignored) {

            }

        }

        return totalATimesB/(Math.sqrt(totalSquareA * totalSquareB));
    }



}
