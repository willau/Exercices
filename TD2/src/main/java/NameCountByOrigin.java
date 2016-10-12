/**
 * Created by willyau on 10/10/16.
 *
 * This Map/Reduce task counts the number of name by origin.
 * When "?" or nothing appears in the area dedicated to origin,
 * we consider it to be unknown and use the key "?" to signify
 * an unknown origin.
 *
 */

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NameCountByOrigin {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Mapper : keys are the origins (String) and values are 1 (Integer)
        private final static IntWritable one = new IntWritable(1);
        private Text origin = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // We split the line into parts and access the origin(s)
            String line = value.toString();
            String[] listOfOrigin = line.split(";")[2].split(",");

            // For each origin, we create a key-value pair (origin, 1)
            for(String originString : listOfOrigin){

                originString = originString.trim();

                // If length > 0, we have an origin string
                if (originString.length() > 0) {
                    origin.set(originString);
                    context.write(origin, one);
                }
                // If length is null, we had a blank space which means unknown origin
                else if (originString.length() == 0 ){
                    // We set the symbol '?' for unknown origin
                    origin.set("?");
                    context.write(origin, one);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Reducer : keys are the origins (String) and values are lists of 1 (Integer)
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // We sum the 1 of the value list to count the number of occurrence of the key origin
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Launching method of Map/Reduce job in main method
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Two path arguments are needed.\n");
        }
        else {
            Job job = Job.getInstance();
            job.setJarByClass(NameCountByOrigin.class);

            job.setJobName("Task 1 - Name Count By Origin");

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(Map.class);
            job.setReducerClass(Reduce.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);
        }
    }
}