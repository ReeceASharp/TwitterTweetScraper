import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

//This is used to cleanup the results from scraper/Twint.py, which are csv files with all of the tweet information inside
public class CSVCleanup {

    public static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text,
            Text> {

        //[X] is optional, and as a result allows the format to be reused for the output to include GMT's 'Z'
        static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss[X]");

        @Override
        protected void setup(Context context) {
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] res = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            // scraped data sometimes processes newline characters left in tweets, which leads to a multiline
            // datapoint. This goes against the structure of CSV, so ignore those and read the rest
            if (res.length != 36)
                return;

            // 36 points in each line:
            // ####################
            // id,conversation_id,created_at,date,time,
            // timezone,user_id,username,name,place,
            // tweet,language,mentions,urls,photos,
            // replies_count,retweets_count,likes_count,hashtags,cashtags,
            // link,retweet,quote_url,video,thumbnail,
            // near,geo,source,user_rt_id,user_rt,
            // retweet_id,reply_to,retweet_date,translate,trans_src,
            // trans_dest
            // ####################

            //grabbing id for redundant data,
            //date + time +

            // I couldn't find any information on the DL sentiment input, so I'm only using English tweets
            // there are also language translation models in a multitude of languages that could be used
            // to translate the other tweets, but the multitude of the tweets are in English, so not much is lost
            if (!res[11].equals("en")) {
                return;
            }

            String id = res[0];

            String tweet = res[10];
            //clean tweet of links, @users, RT's, and normalize it to lowercase
            tweet = tweet.replaceAll("(RT\\s@[A-Za-z]+[A-Za-z0-9-_]+)", "")
                    .replaceAll("(@[A-Za-z]+[A-Za-z0-9-_]+)", "")
                    //replace links + shorteners
                    .replaceAll("http\\S+", "")
                    .replaceAll("bit.ly/\\S+", "")
                    //replace all punctuation
                    .replaceAll("/[^A-Za-z0-9\\s]/g", " ")
                    .replaceAll("/\\s{2,}/g", " ")
                    .toLowerCase();

            //cleanup date, as it's currently staggered by the timezone of the scraper
            // in this case MTC (zone -07), this means converting the date to GMT
            String dateTime = String.format("%s %s", res[3], res[4]);
            ZonedDateTime time = LocalDateTime.parse(dateTime, formatter).atZone(ZoneOffset.ofHours(-7));
            String timeGMT = time.toOffsetDateTime().withOffsetSameInstant(ZoneOffset.UTC).toString();

            String cleanedData = String.format("%s,%s,%s", id, timeGMT, tweet);

            context.write(new Text(id), new Text(cleanedData));

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
        job.setJarByClass(CSVCleanup.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setNumReduceTasks(1);

        //mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //reducer output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}