import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import scala.xml.Null;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class MuskCleaner {

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text,
            NullWritable> {

        //[X] is optional, and as a result allows the format to be reused for the output to include GMT's 'Z'
        static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss[X]");
        static final DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        @Override
        protected void map(Object key, Text value, Context context) throws
                IOException, InterruptedException {
            //get tokens of csv
            String[] tokens = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            //id = 0
            String tweetID = tokens[0];


            //convert timestamp from mountain to GMT in order for the timing to be correct
            //dates are ugly to manipulate
            //date =3
            //time -4
            String date = tokens[3];
            String timestamp = tokens[4];
            if (date.equals("date"))
                return;

            String dateTime = String.format("%s %s", date, timestamp);
            ZonedDateTime time = LocalDateTime.parse(dateTime, formatter).atZone(ZoneOffset.ofHours(-7));
            OffsetDateTime timeGMT = time.toOffsetDateTime().withOffsetSameInstant(ZoneOffset.UTC);
            String dateGMT = outputFormat.format(timeGMT);

            //tweet = 10
            String tweetStr = tokens[10];

            //replies_count = 15
            String replies_count = tokens[15];

            String output = String.format("%s,%s,%s,%s", dateGMT, tweetID, tweetStr, replies_count);
            context.write(new Text(output), NullWritable.get());
        }
    }


    public static class DistinctReducer extends Reducer<Text, IntWritable, Text,
            NullWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context
                context) throws IOException, InterruptedException {


            context.write(key, NullWritable.get());
        }

    }

    public static void main(String[] args) throws Exception {
        /*
        ***********************************
        JOB 1 - Generating Reply List to tweet(s)
        ***********************************
        */
        Configuration conf = new Configuration();

        Job generationJob = Job.getInstance(conf, "Removing Duplicate Tweets");
        generationJob.setJarByClass(MuskCleaner.class);
        generationJob.setMapperClass(MuskCleaner.Mapper.class);
        generationJob.setReducerClass(MuskCleaner.DistinctReducer.class);

        //set 1 reducer
        generationJob.setNumReduceTasks(1);

        //mapper output
        generationJob.setMapOutputKeyClass(Text.class);
        generationJob.setMapOutputValueClass(NullWritable.class);

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
