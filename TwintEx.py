import twint
from datetime import timedelta, date
import threading


# TODO: implement multiple keyword search

# multi-threading class to more fully leverage Twint
# there is downtime between searches as the program outputs the data to file
# in which case more can be used to speed up output
class TwintThread(threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    def run(self):
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
    # musk_tweets()
    # time.sleep(3)
    daily_keyword_tweets()
