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
        StringBuilder intermediateQuestionEight = new StringBuilder();
        Map<String, String> genrePerArtistMap = new HashMap<>();
        Map<String, String> questionEightMap = new HashMap<>();
        HashMap<String, String> questionFiveMap = new HashMap<>();
        HashMap<String, TreeMap<Double, String>> questionFiveTreeMap = new HashMap<>();

        HashMap<String, String> totalLoudnessPerYear = new HashMap<>();
        HashMap<String, String> q9StatsPerYear = new HashMap<>();

        double uniqueCounter = 0.000000001;
        int songsPerArtist = 0;

        double tempoTotal = 0.0;
        int songsWithTempoRecorded = 0;

        StringBuilder q9Hotness = new StringBuilder();
        StringBuilder q9Loudness = new StringBuilder();
        StringBuilder q9Duration = new StringBuilder();
        StringBuilder q9Tempo = new StringBuilder();
        int totalCount = 0;

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
                intermediateQuestionThree.append(cw.getQuestionThree()).append(",,");
            }

            //question 4
            if (!cw.getQuestionFour().isEmpty()) {
                intermediateQuestionFour.append(cw.getQuestionFour());
            }

            //question 5 setup
            if (!cw.getQuestionFive().isEmpty()) {
                String genreTag = cw.getQuestionFive().split(":::")[0];
                String artistTitle = key.toString() + "-" + cw.getQuestionFive().split(":::")[1];
                double songHotness = 0.0;
                try {
                    songHotness = Double.parseDouble(cw.getQuestionFive().split(":::")[2]);
                } catch (NumberFormatException e) {
                    if (cw.getQuestionFive().split(":::")[2].startsWith(":")) {
                        songHotness = Double.parseDouble(cw.getQuestionFive().split(":::")[2].substring(1));
                    }
                }

                if (questionFiveTreeMap.get(genreTag) == null) {
                    TreeMap<Double, String> hotnessArtistTitleMap = new TreeMap<>();
                    hotnessArtistTitleMap.put(songHotness, artistTitle);
                    questionFiveTreeMap.put(genreTag, hotnessArtistTitleMap);
                } else {
                    TreeMap<Double, String> existingMap = questionFiveTreeMap.get(genreTag);

                    if (existingMap.containsKey(songHotness)) {
                        songHotness = songHotness + uniqueCounter;
                        uniqueCounter = uniqueCounter + 0.000000001;

                        existingMap.put(songHotness, artistTitle);
                    } else {
                        existingMap.put(songHotness, artistTitle);
                    }
                }
            }

            //question 6
            if (!cw.getQuestionSix().isEmpty()) {
                //year:loudness:count
                String year = cw.getQuestionSix().split(":::")[0];
                double loudness = Double.parseDouble(cw.getQuestionSix().split(":::")[1]);
                int count = Integer.parseInt(cw.getQuestionSix().split(":::")[2]);

                if (totalLoudnessPerYear.get(year) == null) {
                    totalLoudnessPerYear.put(year, loudness + ":" + count);
                } else {
                    double existingYearLoudness = Double.parseDouble(totalLoudnessPerYear.get(year).split(":")[0]);
                    int existingYearCount = Integer.parseInt(totalLoudnessPerYear.get(year).split(":")[1]);

                    existingYearLoudness += loudness;
                    existingYearCount += count;

                    totalLoudnessPerYear.put(year, existingYearLoudness + ":" + existingYearCount);
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

            //question 9
            if (!cw.getQuestionNine().isEmpty()) {
                double hotness = Double.parseDouble(cw.getQuestionNine().split(":::")[0]);
                double loudness = Double.parseDouble(cw.getQuestionNine().split(":::")[1]);
                double duration = Double.parseDouble(cw.getQuestionNine().split(":::")[2]);
                double tempo = Double.parseDouble(cw.getQuestionNine().split(":::")[3]);
                String year = cw.getQuestionNine().split(":::")[4];
                int count = Integer.parseInt(cw.getQuestionNine().split(":::")[5]);

                if (q9StatsPerYear.get(year) == null) {
                    q9StatsPerYear.put(year, hotness + ":" + loudness + ":" + duration + ":" + tempo + ":" + count);
                } else {
                    double existingYearHotness = Double.parseDouble(q9StatsPerYear.get(year).split(":")[0]);
                    double existingYearLoudness = Double.parseDouble(q9StatsPerYear.get(year).split(":")[1]);
                    double existingYearDuration = Double.parseDouble(q9StatsPerYear.get(year).split(":")[2]);
                    double existingYearTempo = Double.parseDouble(q9StatsPerYear.get(year).split(":")[3]);
                    int existingYearCount = Integer.parseInt(q9StatsPerYear.get(year).split(":")[4]);

                    existingYearHotness += hotness;
                    existingYearLoudness += loudness;
                    existingYearDuration += duration;
                    existingYearTempo += tempo;
                    existingYearCount += count;

                    q9StatsPerYear.put(year, existingYearHotness + ":"
                            + existingYearLoudness + ":" + existingYearDuration + ":" +
                            existingYearTempo + ":" + existingYearCount);
                }
            }

            //q9 correlation between hotness and other measures
            if (!cw.getQuestionNine().isEmpty()) {
                q9Hotness.append(cw.getQuestionNine().split(":::")[0]).append(":::");
                q9Loudness.append(cw.getQuestionNine().split(":::")[1]).append(":::");
                q9Duration.append(cw.getQuestionNine().split(":::")[2]).append(":::");
                q9Tempo.append(cw.getQuestionNine().split(":::")[3]).append(":::");
                totalCount = Integer.parseInt(cw.getQuestionNine().split(":::")[5]);
            }

        }

        StringBuilder q5 = new StringBuilder();
        for (String tag : questionFiveTreeMap.keySet()) {
            TreeMap<Double, String> hotnessArtistTitleMap = questionFiveTreeMap.get(tag);
            NavigableSet<Double> reverseKeys = hotnessArtistTitleMap.descendingKeySet();
            int i = 0;

            q5.append(tag).append("---");
            for (Double hotnessRating : reverseKeys) {
                q5.append(hotnessRating).append(":::")
                        .append(hotnessArtistTitleMap.get(hotnessRating)).append(",,,");
                i++;
                if (i == 10) {break;}
            }
            q5.append("\n");
        }

        //question 6: get all years,loudness:count from map to pass string to reducer
        StringBuilder q6 = new StringBuilder();
        for (String year : totalLoudnessPerYear.keySet()) {
            double yearLoudness = Double.parseDouble(totalLoudnessPerYear.get(year).split(":")[0]);
            int yearCount = Integer.parseInt(totalLoudnessPerYear.get(year).split(":")[1]);

            q6.append(year).append(":").append(yearLoudness).append(":").append(yearCount).append("\n");
        }

        //question 9: get all years' hotness, loudness, duration, tempo, count
        StringBuilder q9 = new StringBuilder();
        for (String year : q9StatsPerYear.keySet()) {
            q9.append(year).append(":").append(q9StatsPerYear.get(year)).append("\n");
        }

        q9Hotness.append("\n");
        q9Loudness.append("\n");
        q9Duration.append("\n");
        q9Tempo.append("\n");

        //q1
        customWritable.setQuestionOne(intermediateQuestionOne.toString());
        //q2
        customWritable.setQuestionTwo(tempoTotal + ":::" + songsWithTempoRecorded);
        //q3
        customWritable.setQuestionThree(intermediateQuestionThree.toString());
        //q4
        customWritable.setQuestionFour(intermediateQuestionFour.toString());
        //q5
        customWritable.setQuestionFive(q5.toString());
        //q6
        customWritable.setQuestionSix(q6.toString());
        //q7
        customWritable.setQuestionSeven(String.valueOf(songsPerArtist));
        //q8
        customWritable.setQuestionEight(intermediateQuestionEight.toString());
        //q9
        customWritable.setQuestionNine(q9.toString());
        //q9p2
        customWritable.setQuestionNineCorrelation(q9Hotness.toString() + q9Loudness.toString()
                + q9Duration.toString() + q9Tempo.toString() + totalCount);

//        customWritable.setFourTest(fourTest);

        context.write(key, customWritable);
    }

}
