import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TimezoneTest {
    public static void main(String[] args) throws ParseException {
        //
        String date = "2012-08-23,20:08:43,-0700";

        String seg[] = date.split(",");
        String newDate = String.format("%s %s", seg[0], seg[1]);
        System.out.println(newDate);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date date_ = sdf.parse(newDate);
        System.out.println(date_);

        sdf.setTimeZone(TimeZone.getTimeZone("MTC"));


        System.out.println(date_);
        //TwitterTweetScraper
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss[X]");
        //DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd kk:mm:ss[ X]");

        // the assumption here is going from MST -> GMT, this could be modularized, but isn't
        // for sake of cost of lookup for each entry
        ZonedDateTime time_ = LocalDateTime.parse(newDate, formatter).atZone(ZoneOffset.ofHours(-7));

        System.out.println(time_);

        System.out.println(time_.toOffsetDateTime().withOffsetSameInstant(ZoneOffset.UTC).format(formatter));
    }

}
