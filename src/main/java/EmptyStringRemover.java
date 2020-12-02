import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class EmptyStringRemover {

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text,
            NullWritable> {

        //remove emojis
        static final String regex = "[^\\p{L}\\p{N}\\p{P}\\p{Z}]";

        @Override
        protected void setup(Context context) {
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] res = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            //if the string is empty, return
            if (res.length != 3)
                return;
            //if the string is just whitespace, or a few characters, disregard those

            String result = res[2].replaceAll(regex, "");

            String trimmedTweet = result.trim();
            if (trimmedTweet.length() < 10)
                return;

            String musk_id = res[0];
            String tweet_id = res[1];

            String cleanedData = String.format("%s,%s,\"%s\"", musk_id, tweet_id, result);

            context.write(new Text(cleanedData), NullWritable.get());

        }
    }

    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text,
            NullWritable>
    {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context
                context) throws IOException, InterruptedException {

            //As the ID of the tweet is a primary key, only output one time to get rid of redundant scraped tweets
            //that were part of multiple keyword searches
            for (Text cleanedData : values) {
                context.write(new Text(cleanedData), NullWritable.get());
                return;
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Twitter Scraper Data Clean");
        job.setJarByClass(EmptyStringRemover.class);
        job.setMapperClass(EmptyStringRemover.Mapper.class);
        job.setReducerClass(EmptyStringRemover.Reducer.class);
        job.setNumReduceTasks(0);

        //mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //reducer output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}