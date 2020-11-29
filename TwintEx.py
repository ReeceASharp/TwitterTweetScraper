import twint
from datetime import timedelta, date
import threading
from pathlib import Path


# TODO: implement multiple keyword search

# multi-threading class to more fully leverage Twint
# there is downtime between searches as the program outputs the data to file
# in which case more can be used to speed up output
class TwintThread(threading.Thread):
    def __init__(self, year):
        threading.Thread.__init__(self)
        self.year = year
        self.name = self.year

        self.limit = 50000

        self.fileName = "temp/" + self.year + "_current_date.txt"

        self.c = twint.Config()
        self.c.Store_csv = True

    def run(self):
        # read in a date from a file, if possible
        # otherwise start from year/1/1

        end_date = "{}-12-31".format(self.year)

        # read in the last completed search, if possible
        # otherwise, start at the beginning of the year
        if Path(self.fileName).exists():
            # read in a line and attempt to convert to a date
            with open(self.fileName, "r") as date_file:
                temp_date = date_file.readLine()
                current_date = date.fromisoformat(temp_date)
        else:
            current_date = "{}-1-1".format(self.year)



        while current_date != end_date:
            for date_, date_next in date_range(current_date, end_date):
                # setup the timeframe for this search
                self.c.Since = date_
                self.c.Until = date_next

                #setup this search's output path
                search_output = "{}_{}_like={}_limit={}.csv".format(date_, date_next, min_likes, self.limit)
                output_path = "E:\\" + search_output
                self.c.Output = output_path

                # perform the search
                twint.run.Search(self.c)

                #write to the file
                with open(self.fileName, 'w') as filetowrite:
                    filetowrite.write(date_)

        print("Starting " + self.name)
        print("Exiting " + self.name)


def musk_tweets():
    keywords = ["elon musk", "tesla"]

    c = twint.Config()
    c.Username = "elonmusk"
    # c.Search = "tesla"

    # output to file, either CSV or JSON for more use later
    c.Output = "out/musk_tweets.csv"
    c.Store_csv = True
    c.Since = "2020-01-20"
    c.Until = "2020-01-29"

    # flags to keep track of
    c.Count = True

    # start the search
    twint.run.Search(c)


# iterate through the dates in YYYY-MM-DD form to input into a keyword search
def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        day = start_date + timedelta(n)
        # For some reason, when staggering the day by only 1, there was no output
        # this causes a larger amount of overlap, but that's easy to filter out in Spark/Mapreduce
        next_day = day + timedelta(days=2)
        yield day.strftime("%Y-%m-%d"), next_day.strftime("%Y-%m-%d")


def daily_keyword_tweets():
    min_likes = 0
    limit = 50000

    c = twint.Config()
    c.Store_csv = True

    # set a minimum like # to ignore bots and get a greater spread over the day
    c.Min_likes = min_likes

    # can be adapted to run through multiple keywords
    c.Search = "tesla"

    # the max amount of tweets to scrape each day
    c.Limit = limit
    c.Count = True

    # start and end date of the scrape
    start_date = date(2019, 2, 24)
    end_date = date.today()

    # iterate through all days and pull relevant data
    for date_, date_next in date_range(start_date, end_date):
        print("FROM: '{}' : '{}' ".format(date_, date_next))

        # set the day that the keyword search will be used on, due to timezones changing this output, it includes 2 days
        c.Since = date_
        c.Until = date_next

        file_name = "{}_{}_like={}_limit={}.csv".format(date_, date_next, min_likes, limit)
        output_path = "E:\\" + file_name
        c.Output = output_path

        twint.run.Search(c)


if __name__ == '__main__':
    min_likes = 0  # only retrieve tweets with at least this amount of likes
    # limit = 50000       # only retrieve this amount of tweets
    search = ["tesla"]  # "elonmusk"
    years = [range(2011, 2021)]

    musk_tweets()
    # time.sleep(3)
    # daily_keyword_tweets()
    threads = []

    for y in years:
        t = TwintThread(y)
        threads.append(t)
        t.start()

    print("Threads have been started")

    for t in threads:
        t.join()

    print("Exiting...")
