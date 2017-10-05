package hadoop.data.analysis.text;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

public class TextMapper extends Mapper<LongWritable, Text, Text, CustomWritable> {

    /**
     * Map program that splits text files and extracts relevant text values. Values are then set
     * per question in a custom Writable object (CustomWritable).
     * @param key state
     * @param value text value of specified substring
     * @param context MapReduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CustomWritable customWritable = new CustomWritable();

        // tokenize into lines.
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {

            String line = itr.nextToken();
            String[] splitLine = line.split("\t");
            String artist = splitLine[11];

            if (line.split("\t")[0].equals("analysis_sample_rate")) {
                continue; //this is the first line in the file and only has header information, skip it
            }

            //question 1: For each artist, what is the most commonly tagged genre?
            String genreTags = splitLine[13];
            genreTags = genreTags.replaceAll("[\\[\\]\"]", "");
            String[] genreArray = genreTags.split(",");
            StringBuilder builder = new StringBuilder();
            for (String genre : genreArray) {
                builder.append(genre);
                builder.append(":");
                builder.append("1");
                builder.append(",");
            }

            customWritable.setQuestionOne(builder.toString());

            //question 8: What are the top ten most popular terms (genres) that songs in the data set have been tagged with?
            customWritable.setQuestionEight(builder.toString());
//
//                //question 2 What is the average tempo across all the songs in the data set?
//                int totalSongs = 1;
//                int tempoScore = Integer.parseInt(line.split("\t")[47]);
//
//                String tempo = totalSongs + ":" + tempoScore;
//                customWritable.setQuestionTwo(tempo);
//
//                //question 3 What is the median danceability score across all the songs in the data set?
//                int hispanicFemalesUnder18 = 0;
//                int hispanicFemaleStartPosition = 4143;
//                for (int i = 0; i < 13; i++) {
//                    hispanicFemalesUnder18 += Integer.parseInt(
//                            line.substring(hispanicFemaleStartPosition, hispanicFemaleStartPosition + 9));
//                    hispanicFemaleStartPosition += 9;
//                }
//                String femalesUnder18 = String.valueOf(hispanicFemalesUnder18);
//
//
//                //question 4 Who are the top ten artists for fast songs (based on their tempo)?
//                int elderlyPopulation = Integer.parseInt(line.substring(1065, 1074));
//                customWritable.setQuestionEight(elderlyPopulation + ":" + totalPopulation);
//
//                //question 5 What are top ten songs based on their hotness in each genre? Please also provide the artist
//                //name and title for these songs.
//
//
//                //question 6: On a per-year basis, what is the mean variance of loudness across the songs within the dataset?
//                int rent = Integer.parseInt(line.substring(1812, 1821));
//                int own = Integer.parseInt(line.substring(1803, 1812));
//
//                String rentVsOwn = rent + ":" + own;
//                customWritable.setQuestionOne(rentVsOwn);

                //question 7: How many songs does each artist have in this data set?
                String songsPerArtist = "1";
                customWritable.setQuestionSeven(songsPerArtist);

//                int houseValueStartPosition = 2928;
//                int totalHomes = 0;
//                String homeValues = "";
//                for (int i = 0; i < 20; i++) {
//                    totalHomes += Integer.parseInt(line.substring(houseValueStartPosition, houseValueStartPosition + 9));
//                    homeValues += Double.parseDouble(line.substring(houseValueStartPosition, houseValueStartPosition + 9)) + ":";
//                    houseValueStartPosition += 9;
//                }
//
//                customWritable.setQuestionFiveTotalHomes(String.valueOf(totalHomes));
//                customWritable.setQuestionFiveHomeValues(homeValues);


            context.write(new Text(artist), customWritable);
        }
    }
}
