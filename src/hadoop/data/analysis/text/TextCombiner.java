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

        StringBuilder intermediateQuestionOne = new StringBuilder();
        StringBuilder intermediateQuestionThree = new StringBuilder();
        StringBuilder intermediateQuestionFour = new StringBuilder();
        StringBuilder intermediateQuestionFive = new StringBuilder();
        StringBuilder intermediateQuestionEight = new StringBuilder();
        Map<String, String> genrePerArtistMap = new HashMap<>();
        Map<String, String> questionEightMap = new HashMap<>();
        HashMap<String, String> questionFiveMap = new HashMap<>();

        int songsPerArtist = 0;

        double tempoTotal = 0.0;
        int songsWithTempoRecorded = 0;

        for (CustomWritable cw : values) {

            //question one
            intermediateStringData = cw.getQuestionOne().split(",,,");

            for (String genrePair : intermediateStringData) {
                try {
                    String genreTag = genrePair.split(":::")[0];
                    double genreCount = Double.parseDouble(genrePair.split(":::")[1]);

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
                intermediateQuestionOne.append(":::");
                intermediateQuestionOne.append(genrePerArtistMap.get(genre));
                intermediateQuestionOne.append(",,,");
            }

            //question two
            if (!cw.getQuestionTwo().startsWith("N/A")) {
                intermediateStringData = cw.getQuestionTwo().split(":::");
                tempoTotal += Double.parseDouble(intermediateStringData[0]);
                songsWithTempoRecorded += Integer.parseInt(intermediateStringData[1]);
            }

            //question three
            if (!cw.getQuestionThree().isEmpty()) {
                intermediateQuestionThree.append(cw.getQuestionThree()).append(",,,");
            }

            //question 4
            if (!cw.getQuestionFour().isEmpty()) {
                intermediateQuestionFour.append(cw.getQuestionFour());
            }

            //question 5 setup
            if (!cw.getQuestionFive().isEmpty()) {
                String[] splitFive = cw.getQuestionFive().split("\n");
                for (String genreTitleHotness : splitFive) {

                    String genreTag = genreTitleHotness.split(":::")[0];
                    String songTitle = genreTitleHotness.split(":::")[1];
                    String songHotness = genreTitleHotness.split(":::")[2];

                    if (questionFiveMap.get(genreTag) == null) {
                        questionFiveMap.put(genreTag, songTitle + ":::" + songHotness);
                    } else {
                        String existingSongsForTag = questionFiveMap.get(genreTag);
                        questionFiveMap.put(genreTag, existingSongsForTag + ",,," +
                                songTitle + ":::" + songHotness);
                    }
                }
            }

            //question 7
            songsPerArtist += Integer.parseInt(cw.getQuestionSeven());

            //question 8 - put the tag and count in a map, updating count as necessary. then turn the map into a string
            intermediateStringData = cw.getQuestionEight().split(",,,");

            for (String allGenrePairs : intermediateStringData) {
                try {
                    String genreTag = allGenrePairs.split(":::")[0];
                    double genreCount = Double.parseDouble(allGenrePairs.split(":::")[1]);

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
                intermediateQuestionEight.append(":::");
                intermediateQuestionEight.append(questionEightMap.get(genre));
                intermediateQuestionEight.append(",,,");
            }

        }

        //question 5: calculate top 10 songs per genre for this artist
        StringBuilder q5 = new StringBuilder();
        for (String genreName : questionFiveMap.keySet()) {
            ArrayList<String> titleHotnessPairs = new ArrayList<>();
            titleHotnessPairs.addAll(Arrays.asList(questionFiveMap.get(genreName).split(",,,")));
            ArrayList<String> tenList = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                double largest = 0;
                String largestString = "";
                for (String titleHotness : titleHotnessPairs) {
                    if (Double.parseDouble(titleHotness.split(":::")[1]) > largest) {
                        largest = Double.parseDouble(titleHotness.split(":::")[1]);
                        largestString = titleHotness.split(":::")[0];
                    }
                }
                tenList.add(largestString + ":::" + largest + ":::" + key.toString());
                titleHotnessPairs.remove(largestString + ":::" + largest);
            }
            q5.append(genreName);
            q5.append("---");
            for (String topTen : tenList) {
                q5.append(topTen).append(",,,");
            }
            q5.append("\n");
        }

//        //q1
        customWritable.setQuestionOne(intermediateQuestionOne.toString());
//        //q2
        customWritable.setQuestionTwo(tempoTotal + ":::" + songsWithTempoRecorded);
//        //q3
        customWritable.setQuestionThree(intermediateQuestionThree.toString());
//        //q4
        customWritable.setQuestionFour(intermediateQuestionFour.toString());
//        //q5
        customWritable.setQuestionFive(q5.toString());
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
