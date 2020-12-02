import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ReplyGenerator {
    public static Boolean DEBUG = false;

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text,
            NullWritable> {

        private final HashSet<String> ids = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException {
            if (DEBUG) System.out.println("\n\nSETTING UP\n\n");
            //insert the adjacency list into memory to quickly access when needed
            // for list appends
            URI[] files = context.getCacheFiles();

            //read in the adjacency list from the cached file
            for (URI file : files) {
                Path path = new Path(file.getPath());
                BufferedReader reader = new BufferedReader(new FileReader(path.getName()));
                String line;

                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

                    ids.add(tokens[0]);
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context) throws
                IOException, InterruptedException {
            String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            if (tokens.length != 36)
                return;

            String groupID = tokens[1];
            String tweet = tokens[10];

            //if this matches one of the IDs of elon musks tweets, then that means it's a reply, output the relevant info
            if (ids.contains(groupID)) {

                tweet = tweet.replaceAll("(RT\\s@[A-Za-z]+[A-Za-z0-9-_]+)", "")
                        .replaceAll("(@[A-Za-z]+[A-Za-z0-9-_]+)", "")
                        //replace links + shorteners
                        .replaceAll("http\\S+", "")
                        .replaceAll("bit.ly/\\S+", "");
                        //replace all punctuation and attempt to remove extra whitespace

                String output = String.format("%s,%s,%s", groupID, tokens[0], tweet);
                context.write(new Text(output), NullWritable.get());
            }
        }
    }

    public static class DistinctReducer extends Reducer<Text, Text, Text,
            NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context
                context) throws IOException, InterruptedException {

            //StringBuilder sb = new StringBuilder();
            for (Text value : values) {

                context.write(new Text(key.toString() + "," + value), NullWritable.get());
                //sb.append(value.toString()).append(",");
            }

            //String output = key.toString() + " " + sb.toString();
            //context.write(new Text(output), NullWritable.get());

        }

    }

    public static void main(String[] args) throws Exception {
        /*
        ***********************************
        JOB 1 - Generating Reply List to tweet(s)
        ***********************************
        */
        Configuration conf = new Configuration();
        //changing the input split sizes because the map is a lengthy process for each bit of data
        //and the data size is extremely small (600KB)
        //conf.set("mapred.max.split.size", "32768");
        //conf.set("mapred.min.split.size", "16384");

        Job generationJob = Job.getInstance(conf, "Generating Replies");
        generationJob.setJarByClass(ReplyGenerator.class);
        generationJob.setMapperClass(ReplyGenerator.Mapper.class);
        generationJob.setReducerClass(ReplyGenerator.DistinctReducer.class);

        //set 1 reducer
        generationJob.setNumReduceTasks(0);

        //mapper output
        generationJob.setMapOutputKeyClass(Text.class);
        generationJob.setMapOutputValueClass(NullWritable.class);

        //setup Distributed Cache
        generationJob.addCacheFile(new URI("hdfs://atlanta:30201/group/data/musk_tweets.csv"));

        //reducer output
        generationJob.setOutputKeyClass(IntWritable.class);
        generationJob.setOutputValueClass(NullWritable.class);

        //file IO
        FileInputFormat.addInputPath(generationJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(generationJob, new Path(args[1]));

        if (!generationJob.waitForCompletion(true))
            System.exit(1);
    }
}

