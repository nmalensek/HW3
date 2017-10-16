package hadoop.data.analysis.text;

import hadoop.data.analysis.util.PearsonCorrelation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

public class TextReducer extends Reducer<Text, CustomWritable, Text, Text> {
    private MultipleOutputs multipleOutputs;
    private HashMap<String, String> totalGenreMap = new HashMap<>();
    private double totalTempo = 0.0;
    private double totalSongsWithTempo = 0.0;
    private TreeMap<Double, Double> danceabilityScores = new TreeMap<>();
    private TreeMap<String, String> totalLoudnessPerYear = new TreeMap<>();
    private TreeMap<String, String> q9StatsPerYear = new TreeMap<>();
    private double uniqueCounter = 0.00000000;
    private double q5uniqueCounter = 0.00000000;
    private double q4uniqueCounter = 0.00000000;
    private String fourTest = "";
    private HashMap<String, TreeMap<Double, String>> q5Map = new HashMap<>();
    private TreeMap<Double, String> q4TreeMap = new TreeMap<>();
    StringBuilder q9Hotness = new StringBuilder();
    StringBuilder q9Loudness = new StringBuilder();
    StringBuilder q9Duration = new StringBuilder();
    StringBuilder q9Tempo = new StringBuilder();
    int q9TotalCount = 0;

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
                "Top 10 artists for fast songs (tempo >= 120 bpm)"), new Text(" \n"));
        multipleOutputs.write("question5", new Text("\nQuestion 5:\n" +
                "Top 10 songs by hotness per genre"), new Text(" \n"));
        multipleOutputs.write("question6", new Text("\nQuestion 6:\n" +
                "Mean loudness variance per year for all songs in the data set"), new Text(" \n"));
        multipleOutputs.write("question7", new Text("\nQuestion 7:\n" +
                "Number of songs by each artist"), new Text(" \n"));
        multipleOutputs.write("question8", new Text("\nQuestion 8:\n" +
                "Top 10 most popular genres songs in the data set are tagged with"), new Text(" \n"));
        multipleOutputs.write("question9", new Text("\nQuestion 9:\n" +
                        "What are the average hotness, loudness, duration, and tempo of songs per year," +
                        " and which of the other measures is hotness most correlated with?"),
                new Text(" \n"));
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

//            if (!cw.getFourTest().isEmpty()) {
//                fourTest = cw.getFourTest();
//            }

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
                String[] splitDanceScores = cw.getQuestionThree().split(",,");
                for (String score : splitDanceScores) {
                    if (danceabilityScores.get(Double.parseDouble(score)) == null) {
                        danceabilityScores.put(Double.parseDouble(score), Double.parseDouble(score));
                    } else {
                        double uniqueScore = (Double.parseDouble(score) + uniqueCounter);
                        danceabilityScores.put(uniqueScore, Double.parseDouble(score));
                        uniqueCounter = uniqueCounter + 0.0000001;
                    }
                }
            }

            //question 4
            if (!cw.getQuestionFour().isEmpty()) {
                String[] splitFastSongs = cw.getQuestionFour().split(",,,");
                for (String song : splitFastSongs) {
                    String artistTitle = key.toString() + "-" + song.split(":::")[0];
                    double tempo = Double.parseDouble(song.split(":::")[1]);

                    if (q4TreeMap.get(tempo) == null) {
                        q4TreeMap.put(tempo, artistTitle);
                    } else {
                        tempo = tempo + q4uniqueCounter;
                        q4uniqueCounter = q4uniqueCounter + 0.00000001;

                        q4TreeMap.put(tempo, artistTitle);
                    }
                }
            }

            //question 5 setup
            if (!cw.getQuestionFive().isEmpty()) {
                for (String genreHotnessArtistTitle : cw.getQuestionFive().split("\n")) {
                    String genreTag = genreHotnessArtistTitle.split("---")[0];
                    for (String hotnessArtistTitle : genreHotnessArtistTitle.split("---")[1].split(",,,")) {
                        double hotness = Double.parseDouble(hotnessArtistTitle.split(":::")[0]);
                        String artistTitle = hotnessArtistTitle.split(":::")[1];

                        if (q5Map.get(genreTag) == null) {
                            TreeMap<Double, String> hotnessArtistTitleMap = new TreeMap<>();
                            hotnessArtistTitleMap.put(hotness, artistTitle);

                            q5Map.put(genreTag, hotnessArtistTitleMap);
                        } else {
                            TreeMap<Double, String> existingHotnessArtistTitleMap = q5Map.get(genreTag);

                            if (existingHotnessArtistTitleMap.containsKey(hotness)) {
                                hotness = hotness + q5uniqueCounter;
                                q5uniqueCounter = q5uniqueCounter + 0.00000001;

                                existingHotnessArtistTitleMap.put(hotness, artistTitle);
                            } else {
                                existingHotnessArtistTitleMap.put(hotness, artistTitle);
                            }
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

            //question 9
            if (!cw.getQuestionNine().isEmpty()) {
                String[] q9YearTotals = cw.getQuestionNine().split("\n");
                for (String q9Data : q9YearTotals) {
                    String year = q9Data.split(":")[0];
                    double hotness = Double.parseDouble(q9Data.split(":")[1]);
                    double loudness = Double.parseDouble(q9Data.split(":")[2]);
                    double duration = Double.parseDouble(q9Data.split(":")[3]);
                    double tempo = Double.parseDouble(q9Data.split(":")[4]);
                    int count = Integer.parseInt(q9Data.split(":")[5]);

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
            }

            //q9p2
            if (!cw.getQuestionNineCorrelation().isEmpty()) {
                q9Hotness.append(cw.getQuestionNineCorrelation().split("\n")[0]).append(":::");
                q9Loudness.append(cw.getQuestionNineCorrelation().split("\n")[1]).append(":::");
                q9Duration.append(cw.getQuestionNineCorrelation().split("\n")[2]).append(":::");
                q9Tempo.append(cw.getQuestionNineCorrelation().split("\n")[3]).append(":::");
                q9TotalCount += Integer.parseInt(cw.getQuestionNineCorrelation().split("\n")[4]);
            }

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

        multipleOutputs.write("question7", key, new Text(" " + songsPerArtist + " songs"));

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
//        questionFiveTopTen(genreHotnessMap);
        multipleOutputs.write("question2", "", new Text(
                "\n" + "Total tempo: " + totalTempo + "\n" + "Songs with tempo: " + totalSongsWithTempo
                        + "\n" + "Average tempo: " + calculateAverage(totalTempo, totalSongsWithTempo)));
        multipleOutputs.write("question3", "", new Text(
                "\nMedian danceability: " + calculateMedian(danceabilityScores)
        + "\n" + "Out of total songs: " + danceabilityScores.size()));
        multipleOutputs.write("question4", "", new Text("\n" + questionFour()));
        multipleOutputs.write("question5", "", new Text("\n" + questionFive()));
        multipleOutputs.write("question6", "", new Text("\n" + questionSix()));
        multipleOutputs.write("question8", "", new Text("\n" + questionEight()));
        multipleOutputs.write("question9", "", new Text("\n" + questionNine()
                + "\n\n" + questionNineCorrelations()));
        super.cleanup(context);
        multipleOutputs.close();
    }

    private String questionFour() {
        StringBuilder fourBuilder = new StringBuilder();
        NavigableSet<Double> highestTempoFirst = q4TreeMap.descendingKeySet();
        int i = 0;

        for (Double tempo : highestTempoFirst) {
            fourBuilder.append(tempo).append(":").append(q4TreeMap.get(tempo)).append("\n");
            i++;
            if (i == 10) {break;}
        }

        return fourBuilder.toString();
    }

    private String questionFive() {
        StringBuilder fiveBuilder = new StringBuilder();

        for (String genreTag : q5Map.keySet()) {
            TreeMap<Double, String> hotnessArtistTitle = q5Map.get(genreTag);
            NavigableSet<Double> mostHotnessFirst = hotnessArtistTitle.descendingKeySet();
            int i = 0;

            fiveBuilder.append(genreTag).append("\n");
            for (Double hotness : mostHotnessFirst) {
                fiveBuilder.append(hotness).append(":").append(hotnessArtistTitle.get(hotness)).append("\n");
                i++;
                if (i == 10) {break;}
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

//    private void questionFiveTopTen(HashMap<String, HashMap<String, String>> fiveMap) {
//        HashMap<String, HashMap<String, String>> fiveMapCopy = new HashMap<>(fiveMap);
//
//        for (String genre : fiveMapCopy.keySet()) {
//            ArrayList<String> topTenList = new ArrayList<>();
//            for (int i = 0; i < 10; i++) {
//                double mostHotness = 0;
//                String hottestTitle = "";
//                String hottestArtist = "";
//
//
//                HashMap<String, String> hottestSongsPerGenre = fiveMapCopy.get(genre);
//                for (String titleArtist : hottestSongsPerGenre.keySet()) {
//                    double hotness = Double.parseDouble(hottestSongsPerGenre.get(titleArtist));
//
//                    if (hotness > mostHotness) {
//                        mostHotness = hotness;
//                        hottestTitle = titleArtist.split(":")[0];
//                        hottestArtist = titleArtist.split(":")[1];
//                    }
//                }
//                fiveMapCopy.get(genre).remove(hottestTitle + ":" + hottestArtist);
//                topTenList.add(mostHotness + ":" + hottestTitle + ":" + hottestArtist);
//            }
//            topTenPerGenre.put(genre, topTenList);
//        }
//    }

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

    private String questionNine() {
        StringBuilder answer = new StringBuilder();
        DecimalFormat decimalFormat = new DecimalFormat("##.00");

        for (String year : q9StatsPerYear.keySet()) {
            double hotness = Double.parseDouble(q9StatsPerYear.get(year).split(":")[0]);
            double loudness = Double.parseDouble(q9StatsPerYear.get(year).split(":")[1]);
            double duration = Double.parseDouble(q9StatsPerYear.get(year).split(":")[2]);
            double tempo = Double.parseDouble(q9StatsPerYear.get(year).split(":")[3]);
            int count = Integer.parseInt(q9StatsPerYear.get(year).split(":")[4]);

            String averageHotness = decimalFormat.format(hotness/count);
            String averageLoudness = decimalFormat.format(loudness/count);
            String averageDuration = decimalFormat.format(duration/count);
            String averageTempo = decimalFormat.format(tempo/count);

            double hotnessLoudnessRatio = (hotness/loudness);
            double hotnessDurationRatio = (hotness/duration);
            double hotnessTempoRatio = (hotness/tempo);

            answer.append(year).append(":").append(averageHotness).append(":").append(averageLoudness).append(":")
                    .append(averageDuration).append(":").append(averageTempo).append(",,,")
                    .append(hotnessLoudnessRatio).append(":").append(hotnessDurationRatio).append(":")
                    .append(hotnessTempoRatio).append("\n");
        }

        return answer.toString();
    }

    private String questionNineCorrelations() {
        PearsonCorrelation correlation = new PearsonCorrelation(
                q9Hotness.toString(), q9Loudness.toString(), q9Duration.toString(),
                q9Tempo.toString(), q9TotalCount);



        correlation.convertToLists();
        correlation.calculateAverages();

        return q9Hotness.toString();
    }

    private String calculateMedian(TreeMap<Double, Double> doubleTreeMap) {
        double median = 0.0;
        int mapSize = doubleTreeMap.size();
        int halfway;
        int upper;
        int currentCount = 0;

        if (mapSize % 2 == 0) {
            halfway = ((mapSize/2) - 1);
            upper = mapSize/2;

            double medianLower = 0;
            double medianUpper = 0;

            for (Double score : doubleTreeMap.keySet()) {
                if (currentCount == halfway) { medianLower = doubleTreeMap.get(score); }
                if (currentCount == upper) {
                    medianUpper = doubleTreeMap.get(score);
                    break;
                }
                currentCount++;
            }

            median = ((medianLower + medianUpper)/2);

        } else {
            halfway = mapSize/2;

            for (Double danceScore : doubleTreeMap.keySet()) {
                if (currentCount == halfway) {
                    median = doubleTreeMap.get(danceScore);
                    break;
                }
                currentCount++;
            }
        }

        return String.valueOf(median);
    }
}
