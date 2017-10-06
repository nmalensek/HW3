package hadoop.data.analysis.test;

import java.util.*;

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

    private ArrayList<String> createList() {
        ArrayList<String> stringList = new ArrayList<>();
        String intermediateStringData = "Folk:1,Rock:1,Country:1,Metal:1,";
        String oneStringData = "Folk:1,Metal:1,";
        String twoStringData = "Folk:1,Rap:1,Metal:1,Hip-Hop:1,";
        stringList.add(oneStringData);
        stringList.add(twoStringData);
        stringList.add(intermediateStringData);

        return stringList;
    }

    private void genreProcessing() {

        ArrayList<String> stringList = createList();
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

    private void getAllGenres() {
        ArrayList<String> testList = createList();
        HashMap<String, String> testMap = new HashMap<>();

        for (String s : testList) {
            String[] chunk = s.split(",");
            loopThroughArray(chunk, testMap);
        }

        System.out.println("expected: Folk:3,Metal:3,Rap:1,Hip-Hop:1,Rock:1,Country:1,");
        System.out.println("Actual:");
        for (String data : testMap.keySet()) {
            System.out.println(data + ":" + testMap.get(data));
        }
    }


    private void loopThroughArray(String[] stringArray, HashMap<String, String> stringMap) {
        for (String genrePair : stringArray) {
            String genreTag = genrePair.split(":")[0];
            int genreCount = Integer.parseInt(genrePair.split(":")[1]);

            if (stringMap.get(genreTag) == null) {
                stringMap.put(genreTag, String.valueOf(genreCount));
            } else {
                int currentCount = Integer.parseInt(stringMap.get(genreTag));
                int newCount = (currentCount + genreCount);
                stringMap.put(genreTag, String.valueOf(newCount));
            }
        }
    }

    private void testResetAndReplace() {
        StringBuilder builder = new StringBuilder();
        builder.append("testetstetestestesttteset");
        builder.delete(0, builder.length());
        System.out.println(builder.toString());

        String emptyArray = "[]";
        emptyArray = emptyArray.replaceAll("[\\[\\]\"]", "");
        if (emptyArray.isEmpty()) {
            System.out.println(emptyArray);
            System.out.println("empty");
        }

        String splitTest = "test,34,532,";
        String[] testsplit = splitTest.split(",");
        System.out.println(Arrays.toString(testsplit));
    }

    public static void main(String[] args) {
        StringManipulationTest stringManipulationTest = new StringManipulationTest();
//        stringManipulationTest.removeBrackets();
//        stringManipulationTest.stringBuilder();
//        stringManipulationTest.genreProcessing();
//        stringManipulationTest.getAllGenres();
        stringManipulationTest.testResetAndReplace();
    }
}
