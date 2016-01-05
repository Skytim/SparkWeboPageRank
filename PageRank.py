import re
import sys
from operator import add

from pyspark import SparkContext

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    parts = re.split(',', urls)
    return parts[2], parts[3]


if __name__ == "__main__":
    sc = SparkContext(appName="PageRank")
    webo_data = sc.textFile("data/week52.csv",1)
    links = webo_data.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    ranks=links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    for iteration in range(5):
    # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    ranks_sort=ranks.sortByKey('true')
    # Collects all URL ranks and dump them to console.
    for (link, rank,) in ranks_sort.take(10):
        if(rank<1):
            print("%s has rank: %s." % (link, rank))   
    def parseNeighbors_csv(urls):
        """Parses a urls pair string into urls pair."""
        parts = re.split(',', urls)
        return parts[2], parts[3]    
    sc.stop()





