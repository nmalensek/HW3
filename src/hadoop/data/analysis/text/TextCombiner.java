package hadoop.data.analysis.text;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class TextCombiner extends Reducer<Text, CustomWritable, Text, CustomWritable> {

    /**
     * Reduces by defining each field and setting the combination of fields necessary
     * to answer each question in a custom Writable object (CustomWritable class).
     * Combiner is used to reduce number of output files and make it easier to write each question answer
     * to its own file.
     *
     * @param key     state
     * @param values  custom Writable object
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {
        CustomWritable customWritable = new CustomWritable();

        String[] intermediateStringData;
        double totalPopulation = 0;
        double unmarriedMales = 0;
        double unmarriedFemales = 0;
        double totalRentals = 0;
        double totalOwners = 0;
        double totalHispanicPopulation = 0;
        double hispanicMalesUnder18 = 0;
        double hispanicFemalesUnder18 = 0;
        double hispanicMales19to29 = 0;
        double hispanicFemales19to29 = 0;
        double hispanicMales30to39 = 0;
        double hispanicFemales30to39 = 0;

        double ruralHouseholds = 0;
        double urbanHouseholds = 0;

        //arrays filled with 0.0 to avoid null pointer exceptions
        double totalHouses = 0;
        String homeValues = "";
        Double[] homeDoubles = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

        double totalRenters = 0;
        String rentValues = "";
        Double[] rentDoubles = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

        StringBuilder intermediateQuestionFour = new StringBuilder();

        StringBuilder intermediateQuestionOne = new StringBuilder();
        StringBuilder intermediateQuestionFive = new StringBuilder();
        StringBuilder intermediateQuestionEight = new StringBuilder();
        Map<String, String> genrePerArtistMap = new HashMap<>();
        Map<String, String> questionEightMap = new HashMap<>();
        HashMap<String, String> questionFiveMap = new HashMap<>();

        int songsPerArtist = 0;

        double tempoTotal = 0.0;
        int songsWithTempoRecorded = 0;

        double urbanPopulation = 0;
        double ruralPopulation = 0;
        double childrenUnder1To11 = 0;
        double children12To17 = 0;
        double hispanicChildrenUnder1To11 = 0;
        double hispanicChildren12To17 = 0;
        double totalMales = 0;
        double totalFemales = 0;

        double elderlyPopulation = 0;

        for (CustomWritable cw : values) {

            //question one
            intermediateStringData = cw.getQuestionOne().split(",");

            for (String genrePair : intermediateStringData) {
                try {
                    String genreTag = genrePair.split(":")[0];
                    double genreCount = Double.parseDouble(genrePair.split(":")[1]);

                    if (genrePerArtistMap.get(genreTag) == null) {
                        genrePerArtistMap.put(genreTag, String.valueOf(genreCount));
                    } else {
                        double currentCount = Double.parseDouble(genrePerArtistMap.get(genreTag));
                        double newCount = (currentCount + genreCount);
                        genrePerArtistMap.put(genreTag, String.valueOf(newCount));
                    }
                } catch (IndexOutOfBoundsException e) {

                }
            }
            for (String genre : genrePerArtistMap.keySet()) {
                intermediateQuestionOne.append(genre);
                intermediateQuestionOne.append(":");
                intermediateQuestionOne.append(genrePerArtistMap.get(genre));
                intermediateQuestionOne.append(",");
            }

            //question two
            if (!cw.getQuestionTwo().startsWith("N/A")) {
                intermediateStringData = cw.getQuestionTwo().split(":");
                tempoTotal += Double.parseDouble(intermediateStringData[0]);
                songsWithTempoRecorded += Integer.parseInt(intermediateStringData[1]);
            }

//            intermediateStringData = cw.getQuestionOne().split(":");
//            totalRentals += Double.parseDouble(intermediateStringData[0]);
//            totalOwners += Double.parseDouble(intermediateStringData[1]);
//
//            intermediateStringData = cw.getQuestionTwo().split(":");
//            unmarriedMales += Double.parseDouble(intermediateStringData[0]);
//            unmarriedFemales += Double.parseDouble(intermediateStringData[1]);
//            totalPopulation += Double.parseDouble(intermediateStringData[2]);
//
//            intermediateStringData = cw.getQuestionThree().split(":");
//            totalHispanicPopulation += Double.parseDouble(intermediateStringData[0]);
//            hispanicMalesUnder18 += Double.parseDouble(intermediateStringData[1]);
//            hispanicMales19to29 += Double.parseDouble(intermediateStringData[2]);
//            hispanicMales30to39 += Double.parseDouble(intermediateStringData[3]);
//            hispanicFemalesUnder18 += Double.parseDouble(intermediateStringData[4]);
//            hispanicFemales19to29 += Double.parseDouble(intermediateStringData[5]);
//            hispanicFemales30to39 += Double.parseDouble(intermediateStringData[6]);
//
//            intermediateStringData = cw.getQuestionFour().split(":");
//            ruralHouseholds += Double.parseDouble(intermediateStringData[0]);
//            urbanHouseholds += Double.parseDouble(intermediateStringData[1]);
//
//            totalHouses += Double.parseDouble(cw.getQuestionFiveTotalHomes());
//            intermediateStringData = cw.getQuestionFiveHomeValues().split(":");
//            for (int i = 0; i < intermediateStringData.length-1; i++) {
//                homeDoubles[i] += Double.parseDouble(intermediateStringData[i]);
//            }
//
//            totalRenters += Double.parseDouble(cw.getQuestionSixTotalRenters());
//            intermediateStringData = cw.getQuestionSixRenterValues().split(":");
//            for (int i = 0; i < intermediateStringData.length; i++) {
//                rentDoubles[i] += Double.parseDouble(intermediateStringData[i]);
//            }

            //question 4
            if (!cw.getQuestionFour().isEmpty()) {
                intermediateQuestionFour.append(cw.getQuestionFour());
            }

            //question 5 setup
            String[] splitFive = cw.getQuestionFive().split(":");
            for (int i = 2; i < splitFive.length; i++) {
                if (questionFiveMap.get(splitFive[i]) == null) {
                    questionFiveMap.put(splitFive[i], splitFive[0] + ":" + splitFive[1]);
                } else {
                    String existingSongsForTag = questionFiveMap.get(splitFive[i]);
                    questionFiveMap.put(splitFive[i], existingSongsForTag + "," + splitFive[0] + ":" + splitFive[1]);
                }
            }

            //question 7
            songsPerArtist += Integer.parseInt(cw.getQuestionSeven());

            //question 8
            intermediateStringData = cw.getQuestionEight().split(",");

            for (String allGenrePairs : intermediateStringData) {
                try {
                    String genreTag = allGenrePairs.split(":")[0];
                    double genreCount = Double.parseDouble(allGenrePairs.split(":")[1]);

                    if (questionEightMap.get(genreTag) == null) {
                        questionEightMap.put(genreTag, String.valueOf(genreCount));
                    } else {
                        double currentCount = Double.parseDouble(questionEightMap.get(genreTag));
                        double newCount = (currentCount + genreCount);
                        questionEightMap.put(genreTag, String.valueOf(newCount));
                    }
                } catch (IndexOutOfBoundsException e) {

                }
            }
            for (String genre : questionEightMap.keySet()) {
                intermediateQuestionEight.append(genre);
                intermediateQuestionEight.append(":");
                intermediateQuestionEight.append(questionEightMap.get(genre));
                intermediateQuestionEight.append(",");
            }

//            intermediateStringData = cw.getQuestionEight().split(":");
//            elderlyPopulation += Double.parseDouble(intermediateStringData[0]);
//
//            intermediateStringData = cw.getQuestionNine().split(":");
//            urbanPopulation += Double.parseDouble(intermediateStringData[0]);
//            ruralPopulation += Double.parseDouble(intermediateStringData[1]);
//            childrenUnder1To11 += Double.parseDouble(intermediateStringData[2]);
//            children12To17 += Double.parseDouble(intermediateStringData[3]);
//            hispanicChildrenUnder1To11 += Double.parseDouble(intermediateStringData[4]);
//            hispanicChildren12To17 += Double.parseDouble(intermediateStringData[5]);
//            totalMales += Double.parseDouble(intermediateStringData[6]);
//            totalFemales += Double.parseDouble(intermediateStringData[7]);
        }

        //calculate 10 fastest songs for this artist
//        StringBuilder fastestSongs = new StringBuilder();
//        if (!intermediateQuestionFour.toString().isEmpty()) {
//            String[] splitFastSongs = intermediateQuestionFour.toString().split(",,,");
//            ArrayList<String> fastSongsCopy = new ArrayList<>(Arrays.asList(splitFastSongs));
//            for (int i = 0; i < 10; i++) {
//                double fastestTempo = 0;
//                String fastestTitle = "";
//                for (String fastSong : fastSongsCopy) {
//                    if (Double.parseDouble(fastSong.split(":::")[1]) > fastestTempo) {
//                        fastestTempo = Double.parseDouble(fastSong.split(":::")[1]);
//                        fastestTitle = fastSong.split(":::")[0];
//                    }
//                }
//                fastestSongs.append(fastestTitle).append(":::").append(fastestTempo).append(",,,");
//                fastSongsCopy.remove(fastestTitle + ":::" + fastestTempo);
//            }
//        }

        //calculate top 10 songs per genre for this artist
//        StringBuilder q5 = new StringBuilder();
//        for (String genreName : questionFiveMap.keySet()) {
//            ArrayList<String> titleHotnessPairs = new ArrayList<>();
//            titleHotnessPairs.addAll(Arrays.asList(questionFiveMap.get(genreName).split(",")));
//            ArrayList<String> tenList = new ArrayList<>();
//
//            for (int i = 0; i < 10; i++) {
//                double largest = 0;
//                String largestString = "";
//                for (String titleHotness : titleHotnessPairs) {
//                    if (Double.parseDouble(titleHotness.split(":")[1]) > largest) {
//                        largest = Double.parseDouble(titleHotness.split(":")[1]);
//                        largestString = titleHotness.split(":")[0];
//                    }
//                }
//                tenList.add(largestString + ":" + largest);
//                titleHotnessPairs.remove(largestString + ":" + largest);
//            }
//            q5.append(genreName);
//            q5.append(":");
//            for (String topTen : tenList) {
//                q5.append(topTen).append(",");
//            }
//            q5.append("\n");
//        }


//        //q1
        customWritable.setQuestionOne(intermediateQuestionOne.toString());
//        //q2
        customWritable.setQuestionTwo(tempoTotal + ":" + songsWithTempoRecorded);
//        //q3
//        customWritable.setQuestionThree(totalHispanicPopulation +":"+hispanicMalesUnder18+":"+hispanicMales19to29+
//        ":"+hispanicMales30to39+":"+hispanicFemalesUnder18+":"+hispanicFemales19to29+":"+hispanicFemales30to39);
//        //q4
        customWritable.setQuestionFour(intermediateQuestionFour.toString());
//        //q5
//        customWritable.setQuestionFive(q5.toString());
//        //q6
//        customWritable.setQuestionSixTotalRenters(String.valueOf(totalRenters));
//        for (int i = 0; i < rentDoubles.length; i++) {
//            rentValues += String.valueOf(rentDoubles[i] + ":");
//        }
//        customWritable.setQuestionSixRenterValues(rentValues);
        //q7
        customWritable.setQuestionSeven(String.valueOf(songsPerArtist));
//        //q8
        customWritable.setQuestionEight(intermediateQuestionEight.toString());
//        //q9
//        customWritable.setQuestionNine(urbanPopulation + ":" + ruralPopulation + ":" + childrenUnder1To11 + ":" +
//        children12To17 + ":" + hispanicChildrenUnder1To11 + ":" + hispanicChildren12To17 + ":" + totalMales +
//        ":" + totalFemales);

        context.write(key, customWritable);
    }

}
