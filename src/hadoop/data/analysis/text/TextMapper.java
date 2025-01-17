package hadoop.data.analysis.text;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.Charset;
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
            String mostTaggedGenre = "";

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
                    if (Double.parseDouble(tagFrequencyArray[i]) == 1.0) {
                        mostTaggedGenre = genreArray[i].startsWith(" ") ?
                                genreArray[i].substring(1) : genreArray[i];

                        builder.append(mostTaggedGenre);
                        builder.append(":::");
                        builder.append(tagFrequencyArray[i]);
                        builder.append(",,,");
                    }

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
            customWritable.setQuestionTwo(tempoScore + ":::" + totalSongs);

            //question 3 What is the median danceability score across all the songs in the data set?

            String danceability = "";
            if (indexIsANumber(splitLine[21])) {
                danceability = splitLine[21];
            }
            customWritable.setQuestionThree(danceability);

            //question 4 Who are the top ten artists for fast songs (based on their tempo)?
            String title = splitLine[50];
            String tempo = splitLine[47];
            String fastSongs = "";
            if (!tempo.startsWith("[") && Double.parseDouble(tempo) >= 250.0 && !title.contains("\t")) {
                fastSongs = title + ":::" + tempo + ",,,";
            }
            customWritable.setQuestionFour(fastSongs);

            //question 6: On a per-year basis, what is the mean variance of loudness across the songs within the data set?

            String loudness = "";
            String year = "";
            loudness = splitLine[27];
            year = splitLine[53];
            try {
                Integer.parseInt(year);
            } catch (NumberFormatException e) {
                year = "";
            }
            customWritable.setQuestionSix(year + ":::" + loudness + ":::" + "1"); //year:loudness:count

            //question 7: How many songs does each artist have in this data set?
            String songsPerArtist = "1";
            customWritable.setQuestionSeven(songsPerArtist);

            //question 8: What are the top ten most popular terms (genres) that songs in the data set have been tagged with?
            for (String s : genreArray) {
                s = s.startsWith(" ") ? s.substring(1) : s; //remove space from start of tag
                builder.append(s);
                builder.append(":::");
                builder.append("1");
                builder.append(",,,");
            }
            customWritable.setQuestionEight(builder.toString());
            builder.delete(0, builder.length());

            //question 5 What are top ten songs based on their hotness in each genre? Please also provide the artist
            //name and title for these songs.
//            if (indexIsANumber(splitLine[42])) {
//                String[] tagsArray = customWritable.getQuestionEight().split(",,,"); //re-use question 8 tag collection
//                for (String genreTag : tagsArray) {
//                    String tag = genreTag.split(":::")[0]; //only get tag out
//                    builder.append(tag);
//                    builder.append(":::");
//                    builder.append(splitLine[50]); //song title
//                    builder.append(":::");
//                    builder.append(splitLine[42]); //song hotness
//                    builder.append(",,,");
//                }
//            }
//            customWritable.setQuestionFive(builder.toString()); //genre:title:hotness\n
//            builder.delete(0, builder.length());

            //question 5 1.0 frequency tag only
            try {
                if (indexIsANumber(splitLine[42])) {
                    builder.append(mostTaggedGenre);
                    builder.append(":::");
                    if (splitLine[50].contains("\t")) {
                        throw new Exception();
                    } else {
                        builder.append(splitLine[50]);
                    }
                    builder.append(":::");
                    builder.append(splitLine[42]);
                }
                customWritable.setQuestionFive(builder.toString());
                builder.delete(0, builder.length());
            } catch (Exception ignored) {

            }

            //q9 what are the hotness to loudness, length, and tempo ratios per year?
            if (indexIsANumber(splitLine[42])) {
                builder.append(splitLine[42]); //hotness
                builder.append(":::");
                builder.append(loudness);
                builder.append(":::");
                builder.append(splitLine[22]); //duration
                builder.append(":::");
                builder.append(splitLine[47]); //tempo
                builder.append(":::");
                builder.append(year);
                builder.append(":::");
                builder.append("1"); //total count
            }
            customWritable.setQuestionNine(builder.toString());
            builder.delete(0, builder.length());

            context.write(new Text(artist), customWritable);
        }
    }

    private boolean indexIsANumber(String shouldBeADouble) {
        try {
            Double.parseDouble(shouldBeADouble);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
