/**
 * Created by willyau on 10/10/16.
 *
 * This Map/Reduce job counts the number of name by origin count.
 * We consider '?' and 'unknown' origin to be of 0 origin count.
 * Therefore, names without explicit origin are considered to have 0 origin.
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


public class NameCountByOriginCount {

    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        // Mapper : keys are the the number of origin found per name (Integer) and value is 1 (Integer)
        private final static IntWritable one = new IntWritable(1);
        private IntWritable originCount = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // We split the line in parts and process the data of a name
            String line = value.toString();
            String[] listOfOrigin = line.split(";")[2].split(",");
            int count = 0 ;

            // We loop to count the number of origins which is 0 by default (counter)
            for(String originString : listOfOrigin) {

                // If we find the string '?', we keep origin to 0
                if (originString.equals("?")){
                    count = 0;
                }
                // Otherwise, when origin is explicit, we increment the counter
                else if (originString.length() > 0) {
                    count += 1;
                }

            }
            originCount.set(count);
            context.write(originCount, one);

        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        // Reducer : keys are origin count (Integer) and values are list of 1 (Integer)
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // We sum the 1 of the value list to count the number of occurrence of the origin count
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
            job.setJarByClass(NameCountByOriginCount.class);

            job.setJobName("Task 2 - Name Count By Origin Count");

            job.setOutputKeyClass(IntWritable.class);
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