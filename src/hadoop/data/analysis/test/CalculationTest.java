package hadoop.data.analysis.test;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class CalculationTest {

    Map<String, Integer> testMap = new HashMap<>();

    public CalculationTest() {

    }

    private void medianTest() {
        TreeMap<Double, Double> scores = new TreeMap<>();
        TreeMap<Double, Double> oddScores = new TreeMap<>();

        Double[] doubles = {20.0, 11.5, 24.5, 100.10, 75.3, 5.1, 40.4, 50.0, 14.0, 9.0, 15.5, 99.3, 23.5, 78.1, 77.0,
        90.0, 43.0, 54.1, 55.5, 80.0};

        for (Double d : doubles) {
            scores.put(d, d);
            oddScores.put(d, d);
        }
        oddScores.put(13.0, 13.0);

        System.out.println(scores.toString());
        System.out.println(oddScores.toString());
        System.out.println(getMedian(scores));
        System.out.println(getMedian(oddScores));


    }

    private String getMedian(TreeMap<Double, Double> treeMap) {
        int mapSize = treeMap.size();
        double median = 0;
        int half;
        int upper;

        if (mapSize % 2 == 0) {
            half = (mapSize/2) - 1;
            upper = (mapSize/2);

            double mLower = 0.0;
            double mUpper = 0.0;
            int currentCount = 0;

            for (Double d : treeMap.keySet()) {
                if (currentCount == half) { mLower = treeMap.get(d); }
                if (currentCount == upper) { mUpper = treeMap.get(d); }
                currentCount++;
            }

            median = ((mLower + mUpper)/2);

        } else {
            half = mapSize/2;

            int currentCount = 0;

            for (Double key : treeMap.keySet()) {
                if (currentCount == half) {
                    median = treeMap.get(key);
                    break;
                }
                currentCount++;
            }

        }

        return String.valueOf(median);
    }

    private void averageTest() {
        double testLong = 100000000;
        int total = 3;

        BigDecimal average = new BigDecimal((testLong / total)).setScale(2, BigDecimal.ROUND_HALF_UP);

        System.out.println(average.toString());
    }

    private void mapTest(String key, Integer value) {

        if (testMap.containsKey(key)) {
            int newValue = testMap.get(key) + value;
            testMap.put(key, newValue);
        } else {
            testMap.put(key, value);
        }

        System.out.println(testMap.get(key));
    }


    public static void main(String[] args) {
        CalculationTest calculationTest = new CalculationTest();
        calculationTest.medianTest();
//        calculationTest.averageTest();
    }


}
