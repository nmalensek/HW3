package hadoop.data.analysis.text;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;

public class TextReducer extends Reducer<Text, CustomWritable, Text, Text> {
    private MultipleOutputs multipleOutputs;
    private HashMap<String, String> totalGenreMap = new HashMap<>();
    private double totalTempo = 0.0;
    private double totalSongsWithTempo = 0.0;
    private HashMap<String, HashMap<String, String>> fastSongsMap = new HashMap<>();
    private HashMap<String, HashMap<String, String>> genreHotnessMap = new HashMap<>();
    private HashMap<String, ArrayList<String>> topTenPerGenre = new HashMap<>();
    private ArrayList<Double> danceabilityScores = new ArrayList<>();
    private HashMap<String, String> totalLoudnessPerYear = new HashMap<>();

    /**
     * Writes answers to each question in their own files.
     *
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    public void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
        multipleOutputs.write("question1", new Text("\nQuestion 1:\n" +
                "Most commonly tagged genre per artist"), new Text(" \n"));
        multipleOutputs.write("question2", new Text("\nQuestion 2:\n" +
                "Average tempo for all songs in the data set"), new Text(" \n"));
        multipleOutputs.write("question3", new Text("\nQuestion 3:\n" +
                "Median danceability score across all songs in the data set"), new Text(" \n"));
        multipleOutputs.write("question4", new Text("\nQuestion 4:\n" +
                "Top 10 artists for fast songs (based on tempo, >= 120 bpm)"), new Text(" \n"));
        multipleOutputs.write("question5", new Text("\nQuestion 5:\n" +
                "Top 10 songs by hotness per genre"), new Text(" \n"));
        multipleOutputs.write("question6", new Text("\nQuestion 6:\n" +
                "Mean loudness variance per year for all songs in the data set"), new Text(" \n"));
        multipleOutputs.write("question7", new Text("\nQuestion 7:\n" +
                "Number of songs by each artist"), new Text(" \n"));
        multipleOutputs.write("question8", new Text("\nQuestion 8:\n" +
                "Top 10 most popular genres songs in the data set are tagged with"), new Text(" \n"));
//        multipleOutputs.write("question9", new Text("\nQuestion 9:\n" +
//                        "Something awesome"),
//                new Text(" \n"));
    }

    /**
     * Sums all values and sets the final values for each variable. Performs calculations as necessary and
     * writes to output file.
     *
     * @param key     state
     * @param values  MapMultiple objects that contain values for each state
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */

    @Override
    protected void reduce(Text key, Iterable<CustomWritable> values, Context context) throws IOException, InterruptedException {
        String[] finalGenreCount;
        String mostTaggedGenre = "";

        int songsPerArtist = 0;
        String[] q8TotalGenreCount;
        HashMap<String, String> genrePerArtistMap = new HashMap<>();
        HashMap<String, String> fastSongsPerArtistMap = new HashMap<>();

        for (CustomWritable cw : values) {

            //question one
            finalGenreCount = cw.getQuestionOne().split(",,,");

            loopThroughArray(finalGenreCount, genrePerArtistMap);

            //question two
            if (!cw.getQuestionTwo().isEmpty()) {
                totalTempo += Double.parseDouble(cw.getQuestionTwo().split(":::")[0]);
                totalSongsWithTempo += Integer.parseInt(cw.getQuestionTwo().split(":::")[1]);
            }

            //question three
            if(!cw.getQuestionThree().isEmpty()) {
                String[] splitDanceScores = cw.getQuestionThree().split(",,,");
                for (String score : splitDanceScores) {
                    try {
                        danceabilityScores.add(Double.parseDouble(score));
                    } catch (NumberFormatException e) {

                    }
                }
            }

            //question 4
            if (!cw.getQuestionFour().isEmpty()) {
                String[] splitFastSongs = cw.getQuestionFour().split(",,,");
                for (String fastSong : splitFastSongs) {
                    String title = fastSong.split(":::")[0];
                    String tempo = fastSong.split(":::")[1];
                    fastSongsPerArtistMap.put(title, tempo);
                }
            }

            //question 5 setup
            if (!cw.getQuestionFive().isEmpty()) {
                String[] splitByGenre = cw.getQuestionFive().split("\n");
                for (String genreTitleHotnessArtist : splitByGenre) {
                    String genre = genreTitleHotnessArtist.split("---")[0];
                    String titleHotnessArtist = genreTitleHotnessArtist.split("---")[1];

                    if (genreHotnessMap.get(genre) == null) {
                        HashMap<String, String> titleArtistHotnessMap = new HashMap<>();
                        for (String tHA : titleHotnessArtist.split(",,,")) {
                            String title = tHA.split(":::")[0];
                            String hotness = tHA.split(":::")[1];
                            String artist = tHA.split(":::")[2];

                            titleArtistHotnessMap.put(title + ":" + artist, hotness);
                        }
                        genreHotnessMap.put(genre, titleArtistHotnessMap);
                    } else {
                        HashMap<String, String> tAHMap = genreHotnessMap.get(genre);
                        for (String nTHA : titleHotnessArtist.split(",,,")) {
                            String nTitle = nTHA.split(":::")[0];
                            String nHotness = nTHA.split(":::")[1];
                            String nArtist = nTHA.split(":::")[2];

                            tAHMap.put(nTitle + ":" + nArtist, nHotness);
                        }
                    }
                }
            }

            //question 6
            if (!cw.getQuestionSix().isEmpty()) {
                String[] yearTotals = cw.getQuestionSix().split("\n");
                for (String yearData : yearTotals) {
                    String year = yearData.split(":")[0];
                    double loudness = Double.parseDouble(yearData.split(":")[1]);
                    int totalCount = Integer.parseInt(yearData.split(":")[2]);

                    if (totalLoudnessPerYear.get(year) == null) {
                        totalLoudnessPerYear.put(year, loudness + ":" + totalCount);
                    } else {
                        String existingLoudnessCount = totalLoudnessPerYear.get(year);
                        double existingLoudness = Double.parseDouble(existingLoudnessCount.split(":")[0]);
                        int existingCount = Integer.parseInt(existingLoudnessCount.split(":")[1]);

                        existingLoudness += loudness;
                        existingCount += totalCount;

                        totalLoudnessPerYear.put(year, existingLoudness + ":" + existingCount);
                    }
                }
            }

            //question 7
            songsPerArtist += Integer.parseInt(cw.getQuestionSeven());

            //question8
            q8TotalGenreCount = cw.getQuestionEight().split(",,,");
            loopThroughArray(q8TotalGenreCount, totalGenreMap);

        }

        //question 1 determine largest
        double largest = 0;
        for (String genre : genrePerArtistMap.keySet()) {
            double genreCount = Double.parseDouble(genrePerArtistMap.get(genre));
            if (genreCount > largest) {
                largest = genreCount;
                mostTaggedGenre = genre + " - " + largest;
            } else if (genreCount == largest) {
                mostTaggedGenre += ", " + genre + " - " + genrePerArtistMap.get(genre);
            }
        }

        //write answers for each artist

        multipleOutputs.write("question1", key, new Text(
                " " + mostTaggedGenre));

        //add all artist's fast songs to map
        fastSongsMap.put(key.toString(), fastSongsPerArtistMap);

        multipleOutputs.write("question7", key, new Text(" " + songsPerArtist + " songs"));

//        multipleOutputs.write("question9", key, new Text(
//                calculatePercentage(urbanPopulation, totalPopulation) + ":" +
//                        calculatePercentage(ruralPopulation, totalPopulation) +
//                        ":" + calculatePercentage(childrenUnder1To11, totalPopulation) +
//                        ":" + calculatePercentage(children12To17, totalPopulation) +
//                        ":" + calculatePercentage(hispanicChildrenUnder1To11, totalPopulation) +
//                        ":" + calculatePercentage(hispanicChildren12To17, totalPopulation) +
//                        ":" + calculatePercentage(totalMales, totalPopulation) +
//                        ":" + calculatePercentage(totalFemales, totalPopulation)));

    }

    /**
     * Close multiple outputs, otherwise the results might not be written to output files.
     * Also writes questions 2, 3, 5, and 8 because the answer isn't per key (artist).
     *
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        questionFiveTopTen(genreHotnessMap);
        multipleOutputs.write("question2", "", new Text(
                "\n" + "Total tempo: " + totalTempo + "\n" + "Songs with tempo: " + totalSongsWithTempo
                        + "\n" + "Average tempo: " + calculateAverage(totalTempo, totalSongsWithTempo)));
        multipleOutputs.write("question3", "", new Text(
                "\nMedian danceability: " + calculateMedian(danceabilityScores)
        + "\n" + "Out of total songs: " + danceabilityScores.size() + "\n" + danceabilityScores.toString()));
        multipleOutputs.write("question4", "", new Text("\n" + questionFour()));
        multipleOutputs.write("question5", "", new Text("\n" + questionFive()));
        multipleOutputs.write("question6", "", new Text("\n" + questionSix()));
        multipleOutputs.write("question8", "", new Text("\n" + questionEight()));
        super.cleanup(context);
        multipleOutputs.close();
    }

    private String questionFour() {
        StringBuilder fourBuilder = new StringBuilder();
        ArrayList<String> topTenFastSongArtists = new ArrayList<>(questionFourTopTen(fastSongsMap));
        for (String tempo : topTenFastSongArtists) {
            fourBuilder.append(tempo).append("\n");
        }
        return fourBuilder.toString();
    }

    private String questionFive() {
        StringBuilder fiveBuilder = new StringBuilder();
        for (String genre : topTenPerGenre.keySet()) {
            fiveBuilder.append(genre).append("\n");
            ArrayList<String> topTen = topTenPerGenre.get(genre);

            for (String song : topTen) {
                fiveBuilder.append(song).append("\n");
            }
            fiveBuilder.append("\n\n");
        }

        return fiveBuilder.toString();
    }

    private String questionEight() {
        //question 8 determine top 10
        StringBuilder builder = new StringBuilder();
        ArrayList<String> topTenGenreTags = new ArrayList<>(calculateTopTen(totalGenreMap));
        for (String tag : topTenGenreTags) {
            builder.append(tag);
            builder.append("\n");
        }

        return builder.toString();
    }

    private String calculateAverage(double numerator, double denominator) {
        DecimalFormat decimalFormat = new DecimalFormat("##.00");
        double average = (numerator / denominator);

        return decimalFormat.format(average);
    }

    private String fastSongsPrint(HashMap<String, String> map) {
        StringBuilder builder = new StringBuilder();
        for (String title : map.keySet()) {
            builder.append(title).append(":").append(map.get(title)).append("\n");
        }
        return builder.toString();
    }

    private void loopThroughArray(String[] stringArray, HashMap<String, String> stringMap) {
        for (String genrePair : stringArray) {
            try {
                String genreTag = genrePair.split(":::")[0];
                double genreCount = Double.parseDouble(genrePair.split(":::")[1]);

                if (stringMap.get(genreTag) == null) {
                    stringMap.put(genreTag, String.valueOf(genreCount));
                } else {
                    double currentCount = Double.parseDouble(stringMap.get(genreTag));
                    double newCount = (currentCount + genreCount);
                    stringMap.put(genreTag, String.valueOf(newCount));
                }
            } catch (ArrayIndexOutOfBoundsException e) {

            }

        }
    }

    private ArrayList<String> calculateTopTen(HashMap<String, String> map) {
        HashMap<String, String> topTenCopy = new HashMap<>(map);
        ArrayList<String> tenList = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            double largest = 0;
            String largestString = "";
            for (String key : topTenCopy.keySet()) {
                if (Double.parseDouble(topTenCopy.get(key)) > largest) {
                    largest = Double.parseDouble(topTenCopy.get(key));
                    largestString = key;
                }
            }
            tenList.add(largestString + ":" + largest);
            topTenCopy.remove(largestString);
        }

        return tenList;
    }

    private ArrayList<String> questionFourTopTen(HashMap<String, HashMap<String, String>> map) {
        HashMap<String, HashMap<String, String>> splittableMapCopy = new HashMap<>(map);
        ArrayList<String> splittableTenList = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            double fastestTempo = 0;
            String fastestTitle = "";
            String fastestArtist = "";
            for (String artist : splittableMapCopy.keySet()) {
                if (splittableMapCopy.get(artist).isEmpty()) {
                    continue;
                }

                HashMap<String, String> fastestSongsPerArtist = splittableMapCopy.get(artist);
                for (String title : fastestSongsPerArtist.keySet()) {
                    double tempo = Double.parseDouble(fastestSongsPerArtist.get(title));

                    if (tempo > fastestTempo) {
                        fastestTempo = tempo;
                        fastestTitle = title;
                        fastestArtist = artist;
                    }
                }
            }
            splittableMapCopy.get(fastestArtist).remove(fastestTitle); //remove fastest song from copied map
            splittableTenList.add(fastestTempo + ":" + fastestTitle + ":" + fastestArtist);
        }
        return splittableTenList;
    }

    private void questionFiveTopTen(HashMap<String, HashMap<String, String>> fiveMap) {
        HashMap<String, HashMap<String, String>> fiveMapCopy = new HashMap<>(fiveMap);

        for (String genre : fiveMapCopy.keySet()) {
            ArrayList<String> topTenList = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                double mostHotness = 0;
                String hottestTitle = "";
                String hottestArtist = "";


                HashMap<String, String> hottestSongsPerGenre = fiveMapCopy.get(genre);
                for (String titleArtist : hottestSongsPerGenre.keySet()) {
                    double hotness = Double.parseDouble(hottestSongsPerGenre.get(titleArtist));

                    if (hotness > mostHotness) {
                        mostHotness = hotness;
                        hottestTitle = titleArtist.split(":")[0];
                        hottestArtist = titleArtist.split(":")[1];
                    }
                }
                fiveMapCopy.get(genre).remove(hottestTitle + ":" + hottestArtist);
                topTenList.add(mostHotness + ":" + hottestTitle + ":" + hottestArtist);
            }
            topTenPerGenre.put(genre, topTenList);
        }
    }

    private String questionSix() {
        StringBuilder answer = new StringBuilder();
        for (String year : totalLoudnessPerYear.keySet()) {
            double yearLoudness = Double.parseDouble(totalLoudnessPerYear.get(year).split(":")[0]);
            int yearCount = Integer.parseInt(totalLoudnessPerYear.get(year).split(":")[1]);

            double average = (yearLoudness/yearCount);
            answer.append(year).append(" average loudness: ").append(average).append("\n");
        }

        return answer.toString();
    }



    private String calculateMedian(ArrayList<Double> doubleList) {
        Collections.sort(doubleList);
        double median;

        if (doubleList.size() % 2 == 0) {
            int lower = (doubleList.size()/2) - 1;
            int upper = (doubleList.size()/2);

            median = (((doubleList.get(lower)) + doubleList.get(upper))/2);

        } else {
            int middle = new BigDecimal(doubleList.size()/2).setScale(2, BigDecimal.ROUND_HALF_UP).intValue();

            median = doubleList.get(middle);
        }


        return String.valueOf(median);
    }
}
