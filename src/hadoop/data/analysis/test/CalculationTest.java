package hadoop.data.analysis.test;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class CalculationTest {

    Map<String, Integer> testMap = new HashMap<>();

    public CalculationTest() {

    }

    private void medianTest() {
        ArrayList<Double> scores = new ArrayList<>();
        ArrayList<Double> oddScores = new ArrayList<>();

        Double[] doubles = {20.0, 11.5, 24.5, 100.10, 75.3, 5.1, 40.4, 50.0, 14.0, 9.0, 15.5, 99.3, 23.5, 78.1, 77.0,
        90.0, 43.0, 54.1, 55.5, 80.0};

        scores.addAll(Arrays.asList(doubles));
        oddScores.addAll(Arrays.asList(doubles));
        oddScores.add(13.0);

        Collections.sort(scores);
        Collections.sort(oddScores);

        System.out.println(scores.toString());
        System.out.println(oddScores.toString());
        System.out.println(getMedian(scores));
        System.out.println(getMedian(oddScores));


    }

    private String getMedian(ArrayList<Double> list) {
        int listSize = list.size();
        double median = 0;

        if (listSize % 2 == 0) {
            int lower = (listSize/2) - 1;
            int upper = (listSize/2);

            median = (((list.get(lower)) + list.get(upper))/2);

        } else {
            int middle = new BigDecimal(listSize/2).setScale(2, BigDecimal.ROUND_HALF_UP).intValue();

            median = list.get(middle);
        }

        return String.valueOf(median);
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
    }


}
