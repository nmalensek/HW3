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
     *
     * @param key     state
     * @param value   text value of specified substring
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
            StringBuilder builder = new StringBuilder();
            String mbGenreTags = splitLine[9];

            try {
                Integer.parseInt(splitLine[0]);
            } catch (NumberFormatException e) {
                continue; //this is the first line in the file and only has header information, skip it
            }

            //question 1: For each artist, what is the most commonly tagged genre?
            String genreTags = "";
            String tagFrequency = "";
            if (splitLine[13].startsWith("\"[\"\"")) {
                genreTags = splitLine[13];
                tagFrequency = splitLine[14];
            } else if (splitLine[14].startsWith("\"[\"\"")) {
                genreTags = splitLine[14];
                tagFrequency = splitLine[15];
            }

            genreTags = genreTags.replaceAll("[\\[\\]\"]", "");
            String[] genreArray = genreTags.split(",");

            tagFrequency = tagFrequency.replaceAll("[\\[\\]]", "");
            String[] tagFrequencyArray = tagFrequency.split(",");

            for (int i = 0; i < tagFrequencyArray.length; i++) {
                try {
                    builder.append(genreArray[i]);
                    builder.append(":");
                    builder.append(tagFrequencyArray[i]);
                    builder.append(",");

                } catch (NumberFormatException e) {
                    //skips files with malformed data
                }
            }

            customWritable.setQuestionOne(builder.toString());
            builder.delete(0, builder.length());


            //question 2 What is the average tempo across all the songs in the data set?
            String tempoScore = "N/A";
            String totalSongs = "N/A";

            if (!splitLine[47].startsWith("[")) {
                tempoScore = splitLine[47];
                totalSongs = "1";
            }
            customWritable.setQuestionTwo(tempoScore + ":" + totalSongs);

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
            //question 4 Who are the top ten artists for fast songs (based on their tempo)?
            String title = splitLine[50];
            String tempo = splitLine[47];
            String fastSongs = "";
            if (!tempo.startsWith("[") && Double.parseDouble(tempo) >= 120.0) {
                fastSongs = title + ":::" + tempo + ",,,";
            }
            customWritable.setQuestionFour(fastSongs);


//                //question 6: On a per-year basis, what is the mean variance of loudness across the songs within the dataset?
//                int rent = Integer.parseInt(line.substring(1812, 1821));
//                int own = Integer.parseInt(line.substring(1803, 1812));
//
//                String rentVsOwn = rent + ":" + own;
//                customWritable.setQuestionOne(rentVsOwn);

            //question 7: How many songs does each artist have in this data set?
            String songsPerArtist = "1";
            customWritable.setQuestionSeven(songsPerArtist);

//            question 8: What are the top ten most popular terms (genres) that songs in the data set have been tagged with?

            mbGenreTags = mbGenreTags.replaceAll("[\\[\\]\"]", "");
            String[] mbArray;
            if (!mbGenreTags.isEmpty()) {
                mbArray = mbGenreTags.split(",");
                for (String mbTag : mbArray) {
                    builder.append(mbTag);
                    builder.append(":");
                    builder.append("1");
                    builder.append(",");
                }
            }
            for (String s : genreArray) {
                builder.append(s);
                builder.append(":");
                builder.append("1");
                builder.append(",");
            }

            customWritable.setQuestionEight(builder.toString());
            builder.delete(0, builder.length());

            //question 5 What are top ten songs based on their hotness in each genre? Please also provide the artist
            //name and title for these songs.

            builder.append(splitLine[50]); //song title
            builder.append(":");
            builder.append(splitLine[42]); //song hotness
            builder.append(":");
            String[] tagsArray = customWritable.getQuestionEight().split(","); //re-use question 8 tag collection
            for (String genreTag : tagsArray) {
                builder.append(genreTag.split(":")[0]);
                builder.append(":");
            }
            customWritable.setQuestionFive(builder.toString()); //title:hotness:tag:tag:...:tag:
            builder.delete(0, builder.length());

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
