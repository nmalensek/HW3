package hadoop.data.analysis.test;

import hadoop.data.analysis.util.PearsonCorrelation;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.TreeMap;

public class CorrelationTest {
    private LinkedList<Double> listA = new LinkedList<>();
    private LinkedList<Double> listB = new LinkedList<>();
    private double originMean = 0.0;
    private double destMean = 0.0;

    public CorrelationTest() {

    }

    double[] aArray = {14.2, 16.4, 11.9, 15.2, 18.5, 22.1, 19.4, 25.1, 23.4, 18.1, 22.6, 17.2};
    Double[] aTestArray = {14.2, 16.4, 11.9, 15.2, 18.5, 22.1, 19.4, 25.1, 23.4, 18.1, 22.6, 17.2};
    double[] bArray = {215.0, 325.0, 185.0, 332.0, 406.0, 522.0, 412.0, 614.0, 544.0, 421.0, 445.0, 408.0};
    Double[] bTestArray = {215.0, 325.0, 185.0, 332.0, 406.0, 522.0, 412.0, 614.0, 544.0, 421.0, 445.0, 408.0};

    private void createLists() {
        listA.addAll(Arrays.asList(aTestArray));
        listB.addAll(Arrays.asList(bTestArray));

        for (Double d : listA) {
            originMean += d;
        }
        originMean = originMean/listA.size();

        for (Double b : listB) {
            destMean += b;
        }
        destMean = destMean/listB.size();

//        System.out.println(originMean);
//        System.out.println(destMean);
//        System.out.println(listA.toString());
//        System.out.println(listB.toString());

    }

    private void apacheTest() {
        PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation();
        System.out.println(pearsonsCorrelation.correlation(aArray, bArray));
    }

    private void test() {
        TreeMap<Integer, Double> subtractedAMap = new TreeMap<>();
        TreeMap<Integer, Double> subtractedBMap = new TreeMap<>();

        TreeMap<Integer, Double> intermediateAMap = new TreeMap<>();
        TreeMap<Integer, Double> intermediateBMap = new TreeMap<>();

        int i = 0;
        for (Double element : listA) {
            double subtractedValue = element - originMean;
            subtractedAMap.put(i, subtractedValue);
            intermediateAMap.put(i, element);
            i++;
        }

        int j = 0;
        for (Double destDouble : listB) {
            double subtracted = destDouble - destMean;
            subtractedBMap.put(j, subtracted);
            intermediateBMap.put(j, destDouble);
            j++;
        }

        double totalSquareA = 0.0;
        for (Double subtractedValue : subtractedAMap.values()) {
            totalSquareA += (subtractedValue * subtractedValue);
        }
//        System.out.println("a: " + totalSquareA);

        double totalSquareB = 0.0;
        for (Double subValue : subtractedBMap.values()) {
            totalSquareB += (subValue * subValue);
        }
//        System.out.println("b: " + totalSquareB);

        double totalATimesB = 0.0;
        for (Integer index : intermediateAMap.keySet()) {
            double a = subtractedAMap.get(index);
            double b = subtractedBMap.get(index);

            totalATimesB += (a * b);
        }
//        System.out.println("ab: " + totalATimesB);

        System.out.println(totalATimesB/(Math.sqrt(totalSquareA * totalSquareB)));
    }

    public static void main(String[] args) {
        CorrelationTest test = new CorrelationTest();
        test.createLists();
        test.test();
        test.apacheTest();
    }
}
