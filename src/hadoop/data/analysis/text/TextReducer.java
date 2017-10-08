package hadoop.data.analysis.text;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.*;

public class TextReducer extends Reducer<Text, CustomWritable, Text, Text> {
    private MultipleOutputs multipleOutputs;
    private HashMap<String, String> totalGenreMap = new HashMap<>();
    double totalTempo = 0.0;
    double totalSongsWithTempo = 0.0;
    private HashMap<String, HashMap<String, String>> fastSongsMap = new HashMap<>();
    private List<Double> averageList = new ArrayList<>();
    private Map<Text, Double> elderlyMap = new HashMap<>();
    private Text mostElderlyState = new Text();
    private double currentMax = 0;

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
//        multipleOutputs.write("question3", new Text("\nQuestion 3:\n" +
//                "Median danceability score across all songs in the data set"), new Text(" \n"));
        multipleOutputs.write("question4", new Text("\nQuestion 4:\n" +
                "Top 10 artists for fast songs (based on tempo, >= 120 bpm)"), new Text(" \n"));
//        multipleOutputs.write("question5", new Text("\nQuestion 5:\n" +
//                "Top 10 songs by hotness per genre"), new Text(" \n"));
//        multipleOutputs.write("question6", new Text("\nQuestion 6:\n" +
//                "Mean loudness variance per year for all songs in the data set"), new Text(" \n"));
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
//        StringBuilder questionFourBuilder = new StringBuilder();

        double totalRent = 0;
        double totalOwn = 0;
        double totalPopulation = 0;
        int fastSongsPerArtist = 0;
        int songsPerArtist = 0;
        String[] q8TotalGenreCount;
        HashMap<String, String> genrePerArtistMap = new HashMap<>();
        HashMap<String, String> fastSongsPerArtistMap = new HashMap<>();

        String[] totalGenreString;
        double urbanPopulation = 0;
        double ruralPopulation = 0;
        double childrenUnder1To11 = 0;
        double children12To17 = 0;
        double hispanicChildrenUnder1To11 = 0;
        double hispanicChildren12To17 = 0;
        double totalMales = 0;
        double totalFemales = 0;

        Double[] homeDoubles = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        Double[] rentDoubles = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        Double[] roomDoubles = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};

        for (CustomWritable cw : values) {

            //question one
            finalGenreCount = cw.getQuestionOne().split(",,,");

            loopThroughArray(finalGenreCount, genrePerArtistMap);

            //question two
            if (!cw.getQuestionTwo().isEmpty()) {
                totalTempo += Double.parseDouble(cw.getQuestionTwo().split(":::")[0]);
                totalSongsWithTempo += Integer.parseInt(cw.getQuestionTwo().split(":::")[1]);
            }

//            totalRent += Double.parseDouble(cw.getQuestionOne().split(":")[0]);
//            totalOwn += Double.parseDouble(cw.getQuestionOne().split(":")[1]);
//
//            totalPopulation += Double.parseDouble(cw.getQuestionTwo().split(":")[0]);
//            totalMalesNeverMarried += Double.parseDouble(cw.getQuestionTwo().split(":")[1]);
//            totalFemalesNeverMarried += Double.parseDouble(cw.getQuestionTwo().split(":")[2]);
//
//            totalHispanicPopulation += Double.parseDouble(cw.getQuestionThree().split(":")[0]);
//            hispanicMalesUnder18 += Double.parseDouble(cw.getQuestionThree().split(":")[1]);
//            hispanicMales19to29 += Double.parseDouble(cw.getQuestionThree().split(":")[2]);
//            hispanicMales30to39 += Double.parseDouble(cw.getQuestionThree().split(":")[3]);
//            hispanicFemalesUnder18 += Double.parseDouble(cw.getQuestionThree().split(":")[4]);
//            hispanicFemales19to29 += Double.parseDouble(cw.getQuestionThree().split(":")[5]);
//            hispanicFemales30to39 += Double.parseDouble(cw.getQuestionThree().split(":")[6]);
//
//            ruralHouseholds += Double.parseDouble(cw.getQuestionFour().split(":")[0]);
//            urbanHouseholds += Double.parseDouble(cw.getQuestionFour().split(":")[1]);
//
//            totalHouses += Double.parseDouble(cw.getQuestionFiveTotalHomes());
//            String[] intermediateStringData = cw.getQuestionFiveHomeValues().split(":");
//            for (int i = 0; i < intermediateStringData.length; i++) {
//                homeDoubles[i] += Double.parseDouble(intermediateStringData[i]);
//            }
//
//            totalRenters += Double.parseDouble(cw.getQuestionSixTotalRenters());
//            intermediateStringData = cw.getQuestionSixRenterValues().split(":");
//            for (int i = 0; i < intermediateStringData.length; i++) {
//                rentDoubles[i] += Double.parseDouble(intermediateStringData[i]);
//            }

            if (!cw.getQuestionFour().isEmpty()) {
                String[] splitFastSongs = cw.getQuestionFour().split(",,,");
                for (String fastSong : splitFastSongs) {
                    String title = fastSong.split(":::")[0];
                    String tempo = fastSong.split(":::")[1];
                    fastSongsPerArtistMap.put(title, tempo);
                }
            }

            songsPerArtist += Integer.parseInt(cw.getQuestionSeven());

            q8TotalGenreCount = cw.getQuestionEight().split(",,,");

            loopThroughArray(q8TotalGenreCount, totalGenreMap);

//            elderlyPopulation += Double.parseDouble(cw.getQuestionEight().split(":")[0]);
//            elderlyMap.put(key, Double.parseDouble(calculatePercentage(elderlyPopulation, totalPopulation)));
//
//            urbanPopulation += Double.parseDouble(cw.getQuestionNine().split(":")[0]);
//            ruralPopulation += Double.parseDouble(cw.getQuestionNine().split(":")[1]);
//            childrenUnder1To11 += Double.parseDouble(cw.getQuestionNine().split(":")[2]);
//            children12To17 += Double.parseDouble(cw.getQuestionNine().split(":")[3]);
//            hispanicChildrenUnder1To11 += Double.parseDouble(cw.getQuestionNine().split(":")[4]);
//            hispanicChildren12To17 += Double.parseDouble(cw.getQuestionNine().split(":")[5]);
//            totalMales += Double.parseDouble(cw.getQuestionNine().split(":")[6]);
//            totalFemales += Double.parseDouble(cw.getQuestionNine().split(":")[7]);

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

        //put home values into an array so they can be put into a map with the ranges
//        for (int i = 0; i < 20; i++) {
//            houseRangeMap.put(houseRanges.getHousingIntegers()[i], homeDoubles[i]);
//        }

        //put rent values into an array so they can be put into a map with the ranges
//        for (int i = 0; i < 17; i++) {
//            rentRangeMap.put(rentRanges.getIntegerRents()[i], rentDoubles[i]);
//        }

        //multiply rooms to get total rooms in state for average calculation
//        for (int i = 0; i < roomDoubles.length; i++) {
//            roomDoubles[i] = (roomDoubles[i] * (i+1));
//        }
//
//        DecimalFormat dF = new DecimalFormat("##.00");
//        double average = calculateAverageRooms(roomDoubles, totalRooms);
//        if (!Double.isNaN(average) && !Double.isInfinite(average)) {
//            double formattedAverage = Double.parseDouble(dF.format(average));
//            averageRooms = formattedAverage;
//        } else {
//            averageRooms = 0;
//        }
////
//        if (averageRooms != 0) {
//            averageList.add(averageRooms);
//        }

        //write answers for each artist

        multipleOutputs.write("question1", key, new Text(
                " " + mostTaggedGenre));


        //add all artist's fast songs to map
        fastSongsMap.put(key.toString(), fastSongsPerArtistMap);
//
//        multipleOutputs.write("question3a", key, new Text(
//                " Males: " + calculatePercentage(hispanicMalesUnder18, totalHispanicPopulation) +
//                        "% | Females: " + calculatePercentage(hispanicFemalesUnder18, totalHispanicPopulation) +
//                        "%"));
//
//        multipleOutputs.write("question3b", key, new Text(
//                " Males: " + calculatePercentage(hispanicMales19to29, totalHispanicPopulation) +
//                        "% | Females: " + calculatePercentage(hispanicFemales19to29, totalHispanicPopulation) +
//                        "%"));
//
//        multipleOutputs.write("question3c", key, new Text(
//                " Males: " + calculatePercentage(hispanicMales30to39, totalHispanicPopulation) +
//                        "% | Females: " + calculatePercentage(hispanicFemales30to39, totalHispanicPopulation) +
//                        "%"));
//
//        multipleOutputs.write("question4", key, new Text(
//                " Rural: " + calculatePercentage(ruralHouseholds, (ruralHouseholds + urbanHouseholds)) +
//                        "% | Urban: " + calculatePercentage(urbanHouseholds, (ruralHouseholds + urbanHouseholds)) +
//                        "%"));

//        multipleOutputs.write("question5", key, new Text(
//                " " + calculateMedian(houseRangeMap, houseRanges.getRanges(), totalHouses)));
//
//        multipleOutputs.write("question6", key, new Text(
//                " " + calculateMedian(rentRangeMap, rentRanges.getRanges(), totalRenters)));

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

//        stateWithMostElderlyPeople(elderlyMap);
    }

    /**
     * Close multiple outputs, otherwise the results might not be written to output files.
     * Also writes questions 2, 3, and 8 because the answer isn't per key (artist).
     *
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.write("question2", "", new Text(
                "\n" + "Total tempo: " + totalTempo + "\n" + "Songs with tempo: " + totalSongsWithTempo
        + "\n" + "Average tempo: " + calculateAverage(totalTempo, totalSongsWithTempo)));
//        multipleOutputs.write("question3", mostElderlyState, new Text(
//                " " + currentMax + "%"));
        multipleOutputs.write("question4", "", new Text("\n" + questionFour()));
        multipleOutputs.write("question8", "", new Text("\n" + questionEight()));
        super.cleanup(context);
        multipleOutputs.close();
    }

    /**
     * Calculate percentage, ignores answer if impossible number is calculated (VI and PR
     * generally cause this)
     *
     * @param numerator
     * @param denominator
     * @return
     */

    private String calculatePercentage(double numerator, double denominator) {
        DecimalFormat decimalFormat = new DecimalFormat("##.00");
        double percentage = (numerator / denominator) * 100;
        if (Double.isInfinite(percentage) || percentage > 100 || percentage < 0) {
            return "N/A";
        } else {
            return decimalFormat.format(percentage);
        }
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
                if (splittableMapCopy.get(artist).isEmpty()) { continue;}

                HashMap<String, String> fastestSongsPerArtist = splittableMapCopy.get(artist);
                for (String title : fastestSongsPerArtist.keySet()) {
                    double tempo = Double.parseDouble(fastestSongsPerArtist.get(title));

//                    if (tempo < fastestTempo) {
//                        break; //songs per artist ordered fastest to slowest, so break on a slower song
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

    private String questionFour() {
        StringBuilder fourBuilder = new StringBuilder();
        ArrayList<String> topTenFastSongArtists = new ArrayList<>(questionFourTopTen(fastSongsMap));
        for (String tempo : topTenFastSongArtists) {
            fourBuilder.append(tempo).append("\n");
        }
        return fourBuilder.toString();
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


    /**
     * Calculates median, returns N/A if no iterations were performed (no data was collected).
     * The current count is tracked because this is calculating the median from ranges, not from
     * each data point.
     *
     * @param map         map of ranges (key) and quantity per range (value)
     * @param dataArray   array of ranges
     * @param totalNumber total number of the variable that's being examined (home values or rent ranges)
     * @return answer
     */

    private String calculateMedian(Map<Integer, Double> map, String[] dataArray, double totalNumber) {
        int currentCount = 0;
        int iterations = 0;

        double dividingPoint = totalNumber * 0.50;

        for (Integer key : map.keySet()) {
            currentCount += map.get(key);
            iterations++;
            if (currentCount > dividingPoint) {
                break;
            }
        }

        String relevantRange = "N/A";

        if (iterations != 0) {
            relevantRange = dataArray[iterations - 1];
        }

//        //debug
//        String test = "";
//        test += iterations + ":" + dividingPoint + ":" + totalNumber + "\n" + map.values().toString() + "\n";
//        for (Integer key : map.keySet()) {
//            test += "[";
//            test += key.toString() + ", ";
//            test += map.get(key) + "]\n";
//        }
//        test += "***" + relevantRange + "***";
        return relevantRange;
    }

    /**
     * Calculates 95th percentile of the given list. If the result of list * .95 divides evenly,
     * that number is the 95th percentile. Otherwise, the next result is in the 95th percentile.
     *
     * @param list list to calculate 95th percentile from
     * @return
     */

    private String calculateNinetyFifthPercentile(List<Double> list) {
        Collections.sort(list);
        BigDecimal ninetyFifthPercentile = null;

        double rawPercentile = list.size() * 0.95;

        if (rawPercentile % 1 == 0) {
            ninetyFifthPercentile = new BigDecimal(rawPercentile).setScale(0);
        }
        if (rawPercentile % 1 != 0) {
            ninetyFifthPercentile = new BigDecimal(rawPercentile).setScale(0, BigDecimal.ROUND_UP);
        }
        int ninetyFifthPercentilePosition = ninetyFifthPercentile.intValueExact();

        double ninetyFifthPercentileNumber = list.get(ninetyFifthPercentilePosition - 1);

        String answer = Double.toString(ninetyFifthPercentileNumber);
//        debug
//        String test = "";
//        test += ninetyFifthPercentile + ":" + ninetyFifthPercentilePosition + "\n" + list.toString() + "\n";
//        test += list.size() + "\n";
//        test += "***" + ninetyFifthPercentileNumber + "***";

        return answer;
    }

//    private double calculateAverageRooms(Double[] rooms, double totalHouses) {
//        double actualRoomQuantity = 0;
//        for (int i = 0; i < 9; i++) {
//            actualRoomQuantity += rooms[i];
//        }
//        return  actualRoomQuantity / totalHouses;
//    }
//
//    /**
//     * Checks if the percentage of elderly population in the state is the most compared to all other
//     * states analyzed so far.
//     * @param stateElderlyMap Map of states' elderly population percentages
//     */
//
//
//    private void stateWithMostElderlyPeople(Map<Text, Double> stateElderlyMap) {
//        for (Text state : stateElderlyMap.keySet()) {
//            if (stateElderlyMap.get(state) > currentMax) {
//                currentMax = stateElderlyMap.get(state);
//                mostElderlyState.set(state);
//            }
//        }
//    }
}
