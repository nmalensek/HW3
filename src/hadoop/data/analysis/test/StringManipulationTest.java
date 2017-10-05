package hadoop.data.analysis.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringManipulationTest {


    private  void removeBrackets() {
        String bracketTest = "[\"\"elem1\"\", \"\"elem2\"\", \"\"elem3\"\"]";
        System.out.println(bracketTest);
        bracketTest = bracketTest.replaceAll("[\\[\\]\"]", "");
        System.out.println(bracketTest);
    }

    private void stringBuilder() {
        StringBuilder builder = new StringBuilder();
        builder.append("test");
        builder.append(":");
        builder.append("1");
        builder.append(",");
        System.out.println(builder.toString());
    }

    private void genreProcessing() {
        String intermediateStringData = "Folk:1,Rock:1,Country:1,Metal:1,";
        List<String> stringList = new ArrayList<>();
        String oneStringData = "Folk:1,Metal:1,";
        String twoStringData = "Folk:1,Rap:1,Metal:1,Hip-Hop:1,";
        stringList.add(oneStringData);
        stringList.add(twoStringData);
        stringList.add(intermediateStringData);

        StringBuilder builder = new StringBuilder();
        Map<String, String> genrePerArtistMap = new HashMap<>();
//        List<String> taggedGenres = new ArrayList<>();
//        for (String element : stringList) {
//            String[] splitData = element.split(",");
//            for (String genrePair : splitData) {
//                boolean newGenre = true;
//                for (int i = 0; i < taggedGenres.size(); i++) {
//                    if (genrePair.split(":")[0].equals(taggedGenres.get(i).split(":")[0])) {
//                        newGenre = false;
//                        int currentCount = Integer.parseInt(taggedGenres.get(i).split(":")[1]);
//                        int newCount = currentCount + Integer.parseInt(genrePair.split(":")[1]);
//                        taggedGenres.set(i, taggedGenres.get(i).split(":")[0] + ":" + String.valueOf(newCount));
//                    }
//                }
//                if (newGenre) {
//                    taggedGenres.add(genrePair);
//                }
//            }
//        }
//        for (String element : taggedGenres) {
//            builder.append(element);
//            builder.append(",");
//        }
        for (String genrePair : stringList) {
            String[] splitData = genrePair.split(",");
            for (String splitPair : splitData) {
                if (genrePerArtistMap.get(splitPair.split(":")[0]) == null) {
                    genrePerArtistMap.put(splitPair.split(":")[0], splitPair.split(":")[1]);
//                    System.out.println(splitPair.split(":")[0] + ":" + splitPair.split(":")[1]);
                } else {
                    int currentCount = Integer.parseInt(genrePerArtistMap.get(splitPair.split(":")[0]));
                    int newCount = currentCount + Integer.parseInt(splitPair.split(":")[1]);
                    genrePerArtistMap.put(splitPair.split(":")[0], String.valueOf(newCount));
//                    System.out.println(splitPair.split(":")[0] + ":" + newCount);
                }
            }
        }
        for (String genre : genrePerArtistMap.keySet()) {
            builder.append(genre);
            builder.append(":");
            builder.append(genrePerArtistMap.get(genre));
            builder.append(",");
        }

        String mostTaggedGenre = "";
        int largest = 0;
        for (String tagged : genrePerArtistMap.keySet()) {
            int genreCount = Integer.parseInt(genrePerArtistMap.get(tagged));
            if (genreCount > largest) {
                largest = genreCount;
                mostTaggedGenre = tagged + " tagged " + largest + " times";
            } else if (genreCount == largest) {
                mostTaggedGenre += ", " + tagged + " tagged " + largest + " times";
            }
        }
        System.out.println("Expected: Folk tagged 3 times, Metal tagged 3 times");
        System.out.println("Actual: " + mostTaggedGenre);
//        System.out.println("expected: Folk:3,Metal:3,Rap:1,Hip-Hop:1,Rock:1,Country:1,");
//        System.out.println("actual: " + builder.toString());

    }

    public static void main(String[] args) {
        StringManipulationTest stringManipulationTest = new StringManipulationTest();
//        stringManipulationTest.removeBrackets();
//        stringManipulationTest.stringBuilder();
        stringManipulationTest.genreProcessing();
    }
}
