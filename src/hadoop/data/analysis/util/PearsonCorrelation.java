package hadoop.data.analysis.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;

public class PearsonCorrelation {

    private String hotness;
    private String loudness;
    private String duration;
    private String tempo;
    private int totalSongs;
    private LinkedList<Double> hotnessList = new LinkedList<>();
    private LinkedList<Double> loudnessList = new LinkedList<>();
    private LinkedList<Double> durationList = new LinkedList<>();
    private LinkedList<Double> tempoList = new LinkedList<>();
    private double hotnessAverage;
    private double loudnessAverage;
    private double durationAverage;
    private double tempoAverage;
    private HashMap<Double, Double> sumHotnessMap = new HashMap<>();
    private HashMap<Double, Double> sumLoudnessMap = new HashMap<>();
    private HashMap<Double, Double> sumDurationMap = new HashMap<>();
    private HashMap<Double, Double> sumTempoMap = new HashMap<>();

    public PearsonCorrelation(String hotness, String loudness, String duration, String tempo, int totalSongs) {
            this.hotness = hotness;
            this.loudness = loudness;
            this.duration = duration;
            this.tempo = tempo;
            this.totalSongs = totalSongs;
    }

    public void convertToLists() {
        convertStringToDoubleList(hotness, hotnessList);
        convertStringToDoubleList(loudness, loudnessList);
        convertStringToDoubleList(duration, durationList);
        convertStringToDoubleList(tempo, tempoList);
    }

    public void calculateAverages() {
        hotnessAverage = averageForAList(hotnessList);
        loudnessAverage = averageForAList(loudnessList);
        durationAverage = averageForAList(durationList);
        tempoAverage = averageForAList(tempoList);
    }

    public void getSums() {

    }

    private void convertStringToDoubleList(String originString, LinkedList<Double> destList) {
        for (String element : originString.split(":::")) {
            destList.add(Double.parseDouble(element));
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

    private double sumFromList(LinkedList<Double> originList, LinkedList<Double> destList,
                                          double originMean, double destMean) {
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
            double a = intermediateAMap.get(index);
            double b = intermediateBMap.get(index);

            totalATimesB += (a * b);
        }

        return totalATimesB/(Math.sqrt(totalSquareA * totalSquareB));
    }



}
