# TwitterTweetScraper

This was an attempt to collect the majority of the tweets on Twitter relating to Tesla and Elon Musk by leverating the scraping tool Twint. At the moment the scraper simply uses a thread for each year, but this will be changed to utilize a threadpool for a more even throughput. It also attempts to break the data up by day, and will maintain the current progress. My internet isn't the best, and I needed some way to make sure the search timeouts didn't cause an early exit.

This data is then fed into a variety of MapReduce programs to produce more specific subsets of the data. One being all replies to a designated user, and another being a list of cleaned data points after being fed through some regular expressions. 

From there, this could be input into either training models from scratch like the ones offered by Spark NLP, or as an input to pipelines to receive a final output like in the form of a sentiment analysis.
