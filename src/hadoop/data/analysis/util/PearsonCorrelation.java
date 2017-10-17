package hadoop.data.analysis.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;

public class PearsonCorrelation {

    private LinkedList<Double> hotnessList = new LinkedList<>();
    private LinkedList<Double> loudnessList = new LinkedList<>();
    private LinkedList<Double> durationList = new LinkedList<>();
    private LinkedList<Double> tempoList = new LinkedList<>();
    private double hotnessAverage;
    private double loudnessAverage;
    private double durationAverage;
    private double tempoAverage;
    private double hotnessLoudness;
    private double hotnessDuration;
    private double hotnessTempo;

    public PearsonCorrelation(LinkedList<Double> hotness, LinkedList<Double> loudness,
                              LinkedList<Double> duration, LinkedList<Double> tempo) {
        hotnessList = hotness;
        loudnessList = loudness;
        durationList = duration;
        tempoList = tempo;
    }

    public void calculateAverages() {
        hotnessAverage = averageForAList(hotnessList);
        loudnessAverage = averageForAList(loudnessList);
        durationAverage = averageForAList(durationList);
        tempoAverage = averageForAList(tempoList);
    }

    public String getCorrelations() {
        StringBuilder correlations = new StringBuilder();
        hotnessLoudness = correlationFromList(hotnessList, hotnessAverage, loudnessList, loudnessAverage);
        hotnessDuration = correlationFromList(hotnessList, hotnessAverage, durationList, durationAverage);
        hotnessTempo = correlationFromList(hotnessList, hotnessAverage, tempoList, tempoAverage);

        correlations.append("Hotness:Loudness correlation: ").append(hotnessLoudness).append("\n");
        correlations.append("Hotness:Duration correlation: ").append(hotnessDuration).append("\n");
        correlations.append("Hotness:Tempo correlation: ").append(hotnessTempo).append("\n");

        correlations.append("\n");
        correlations.append(hotnessAverage).append("\n").append(loudnessAverage).append("\n")
                .append(durationAverage).append("\n").append(tempoAverage);
        correlations.append("\n\n").append(hotnessList.size()).append("\n").append(tempoList.size());
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
        TreeMap<Integer, Double> intermediateBMap = new TreeMap<>();

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
            intermediateBMap.put(j, destDouble);
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
            double a = subtractedAMap.get(index);
            double b = subtractedBMap.get(index);

            totalATimesB += (a * b);
        }

        return totalATimesB/(Math.sqrt(totalSquareA * totalSquareB));
    }



}
