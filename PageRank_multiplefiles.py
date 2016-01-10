#encoding=utf-8
import re
import sys
import time
from operator import add

from pyspark import SparkConf, SparkContext

def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    parts = re.split(',', urls)
    return parts[2], parts[3]


if __name__ == "__main__":
    tStart = time.time()
    # conf = (SparkConf().setMaster("local[4]").setAppName("PageRank"))
    sc = SparkContext(appName="PageRank")
    webo_data = sc.textFile("data/webodata/*.csv",1)
    links = webo_data.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    ranks=links.map(lambda url_neighbors: (url_neighbors[0], 1.0))
    for iteration in range(5):
    # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
    # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    ranks_sort=ranks.takeOrdered(100,key = lambda x: x[1])
    for sortData in ranks_sort:
        print sortData
    tStop = time.time()
    print (str(tStop-tStart)+"s")
    print (webo_data.count()+"筆數")
    sc.stop()







