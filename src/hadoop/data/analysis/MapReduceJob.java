package hadoop.data.analysis;

import hadoop.data.analysis.text.CustomWritable;
import hadoop.data.analysis.text.TextCombiner;
import hadoop.data.analysis.text.TextMapper;
import hadoop.data.analysis.text.TextReducer;
import hadoop.data.analysis.util.PrintResults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class MapReduceJob {

    private static Configuration conf = new Configuration();

    /**
     * Executes map reduce job.
     * @param args first argument is input path, second is output path, and third is desired behavior
     *             (run the job or print results)
     */

    private void executeJob(String[] args) {
        try {
            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(conf);

            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }

            //job output key/value separated by : instead of default \t
            conf.set("mapreduce.output.textoutputformat.separator", ":");
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "million songs analysis");
//            job.setNumReduceTasks(2);
            // Current class.
            job.setJarByClass(MapReduceJob.class);
            // Mapper
            job.setMapperClass(TextMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(TextCombiner.class);
            // Reducer
            job.setReducerClass(TextReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(CustomWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, inputPath);
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, outputPath);

            addCustomDirectories(job);
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    /**
     * Parses result files and prints their contents.
     * @param args main method argument output path
     * @throws IOException
     */

    private void printResults(String[] args) throws IOException {
        Path outputPath = new Path(args[1]);
        PrintResults printResults = new PrintResults(outputPath, conf);
        printResults.printOutput();
    }

    /**
     * Write each question in its own output folder
     * @param job Hadoop job
     */
    private void addCustomDirectories(Job job) {
        MultipleOutputs.addNamedOutput(job, "question1", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "question2", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "question3", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "question4", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "question5", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "question6", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "question7", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "question8", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "question9", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "question9b", TextOutputFormat.class, Text.class, Text.class);
    }

    public static void main(String[] args) throws IOException {
        MapReduceJob mapReduceJob = new MapReduceJob();
        if (args[2].equals("go")) mapReduceJob.executeJob(args);
        if (args[2].equals("print")) mapReduceJob.printResults(args);
    }
}
