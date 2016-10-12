/**
 * Created by willyau on 10/10/16.
 *
 * This Map/Reduce job computes the percentage of male name and female name.
 * To simplify the computation, when the same name appears on two different
 * lines, we consider it to be two different names.
 *
 */


import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class NameGenderProp {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Mapper : keys are gender (String) and values are 1 (Integer)
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);
        private final static Text female = new Text("Female Name");
        private final static Text male = new Text("Male Name");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // We split line to process the gender information
            String line = value.toString();
            String[] genders = line.split(";")[1].split(",");

            // We loop on each gender (1 or 2 gender)
            for(String genderString : genders){
                genderString = genderString.trim();
                // If gender is female, we output two pairs : ('f',1) and ('m',0)
                if(genderString.equals("f")){
                    context.write(female, one);
                    context.write(male, zero);
                // If gender is female, we output two pairs : ('m',1) and ('f',0)
                }else if(genderString.equals("m")){
                    context.write(male, one);
                    context.write(female, zero);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, FloatWritable> {

        // Reducer : keys are gender (String) and values are list of 0 and 1 (Integer)
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // We sum the 0 and 1 to find the number of occurrence of the gender
            int sum = 0;
            // We want the size of the list which is the total of names, regardless of gender
            int size = 0;
            for (IntWritable val : values) {
                sum += val.get();
                size += 1;
            }
            // We compute the percentage for the gender by dividing gender occurrence by total or names
            float proportion = 100 * (float)sum / (float)size ;
            context.write(key, new FloatWritable(proportion));
        }
    }

    // Launching method of Map/Reduce job in main method
    public static void main(String[] args) throws Exception {

        if (args.length != 2){
            System.err.printf("Two path arguments are needed.\n");
        }
        else {
            Job job = Job.getInstance();
            job.setJarByClass(NameCountByOrigin.class);

            job.setJobName("Task 3 - Name Gender Proportion");

            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputValueClass(FloatWritable.class);

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
