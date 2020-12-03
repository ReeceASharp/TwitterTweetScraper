import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentCounter {

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text,
            IntWritable> {

        @Override
        protected void setup(Context context) {

        }

        @Override
        protected void map(Object key, Text value, Context context) throws
                IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String replyID = tokens[0];
            String muskID = tokens[1];
            String[] sentiments = tokens[2].split("\\|");

            for (String sentiment : sentiments) {
                if (sentiment.equals("na"))
                    continue;
                int temp = 0;
                if (sentiment.equals("negative"))
                    temp = -1;
                if (sentiment.equals("positive"))
                    temp = 1;
                context.write(new Text(muskID), new IntWritable(temp));
            }
        }
    }


    public static class DistinctReducer extends Reducer<Text, IntWritable, Text,
            NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context
                context) throws IOException, InterruptedException {

            int positive = 0;
            int negative = 0;
            for (IntWritable value : values) {
                switch(value.get()) {
                    case 1:
                        positive += 1;
                        break;
                    case -1:
                        negative += 1;
                        break;
                    default:
                        break;
                }
            }
            String output = String.format("%s,%d,%d", key.toString(), negative, positive);

            context.write(new Text(output), NullWritable.get());
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job generationJob = Job.getInstance(conf, "Removing Duplicate Tweets");
        generationJob.setJarByClass(SentimentCounter.class);
        generationJob.setMapperClass(SentimentCounter.Mapper.class);
        generationJob.setReducerClass(SentimentCounter.DistinctReducer.class);

        //set 1 reducer
        generationJob.setNumReduceTasks(1);

        //mapper output
        generationJob.setMapOutputKeyClass(Text.class);
        generationJob.setMapOutputValueClass(IntWritable.class);

        //reducer output
        generationJob.setOutputKeyClass(Text.class);
        generationJob.setOutputValueClass(NullWritable.class);

        //file IO
        FileInputFormat.addInputPath(generationJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(generationJob, new Path(args[1]));

        if (!generationJob.waitForCompletion(true))
            System.exit(1);
    }
}