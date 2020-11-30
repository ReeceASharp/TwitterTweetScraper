import twint
from datetime import timedelta, date
import threading
from pathlib import Path
from time import sleep
import os


# multi-threading class to more fully leverage Twint
# there is downtime between searches as the program outputs the data to file
# in which case more can be used to speed up output
class TwintThread(threading.Thread):
    def __init__(self, year, keywords, output):
        threading.Thread.__init__(self)

        # search parameters
        self.year = year
        self.name = self.year
        self.keywords = keywords
        self.limit = 0
        self.min_likes = 0

        # file I/O parameters
        self.outputPath = output + str(self.year) + '\\'
        self.fileName = "temp/" + str(self.year) + "_current_date.txt"

        # Search config and flag
        self.c = twint.Config()
        self.c.Store_csv = True
        self.c.Hide_output = True
        self.c.Count = True

        # generation of end point, exclusive
        self.end_date = date(self.year + 1, 1, 1)

    def run(self):
        # attempt to create a folder to store the outputs, could already exist from prior runs, in which case ignore
        try:
            os.mkdir(self.outputPath)
        except:
            pass

        # read in the last completed search, if possible
        if Path(self.fileName).exists():
            # read in a line and attempt to convert to a date, start on the next day
            with open(self.fileName, "r") as date_file:
                temp_date = date_file.readline()
                current_date = date.fromisoformat(temp_date) + timedelta(days=1)
        else:
            # otherwise, start at the beginning of the year
            current_date = date(self.year, 1, 1)

        while current_date < self.end_date:
            for date_, date_next in date_range(current_date, self.end_date):
                try:
                    for keyword in self.keywords:
                        print("{} - {} - {}".format(self.name, date_, keyword))
                        # setup the timeframe for this search
                        self.c.Since = date_
                        self.c.Until = date_next
                        self.c.Search = keyword

                        # setup this search's output path
                        search_output = "{}_{}_like={}_limit={}_keyword={}.csv".format(date_, date_next, self.min_likes,
                                                                                       self.limit, keyword)
                        final_path = self.outputPath + search_output
                        self.c.Output = final_path

                        # perform the search
                        twint.run.Search(self.c)

                    # overwrite the day with the new finished day
                    with open(self.fileName, 'w') as file_to_write:
                        file_to_write.write(date_)
                except:
                    # this is used as a fail-safe on network failure
                    # there is a 30 second time-out on Twint, but this
                    # just attempts the search again after 60 seconds
                    # in order to ensure it runs over a long period of time
                    print("Network Connection Failure. Waiting 60 seconds before starting again.")
                    sleep(30)
                    break
                else:
                    # update current_date for the while conditional above
                    current_date = date.fromisoformat(date_) + timedelta(days=1)

            print("{} != {}".format(current_date, self.end_date))
        print("Finished ", self.year)


# iterate through the dates in YYYY-MM-DD form to input into a keyword search
def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        day = start_date + timedelta(n)
        # For some reason, when staggering the day by only 1, there was no output
        # this causes a larger amount of overlap, but that's easy to filter out in Spark/Mapreduce
        next_day = day + timedelta(days=2)
        yield day.strftime("%Y-%m-%d"), next_day.strftime("%Y-%m-%d")


def musk_tweets():

    c = twint.Config()
    c.Username = "elonmusk"

    # output to file, either CSV or JSON for more use later
    c.Output = "out/musk_tweets.csv"
    c.Store_csv = True

    # flags to keep track of
    c.Count = True

    # start the search
    twint.run.Search(c)


if __name__ == '__main__':
    # search through years with keyword search, creates a thread for each year, could be refactored to
    # multi-thread on a smaller increment, like a month, week, or day
    years = list(range(2011, 2021))
    search = ["tesla", "elonmusk"]
    output_path = "E:\\"

    # setup multiple threads, one for each year
    threads = []
    for y in years:
        t = TwintThread(y, search, output_path)
        threads.append(t)
        t.start()

    # wait until they're all done
    for t in threads:
        t.join()

    # async cleanup error, possibly an error only on Windows, doesn't affect output and only occurs on thread exit
    # a possible fix is just to give the graphs time to catch up, in which case a wait-time is used
    # https://github.com/encode/httpx/issues/914
    print("Exiting...")
    sleep(5)
