#%%
# !pip install pyspark
#%%
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Assignment2 MapReduce").getOrCreate()
sc = spark.sparkContext

#%%
# from google.colab import drive
# drive.mount('/content/drive')
#
# rdd = sc.textFile("/content/drive/MyDrive/pagecounts-20160101-000000_parsed.out")
# print(rdd.first()) # just to make sure the file is read

rdd = sc.textFile("work/FCAI/BigData/spark-ass2/pagecounts-20160101-000000_parsed.out")
print(rdd.first())  # just to make sure the file is read
#%%
import time
import re


# line: ProjectCode PageTitle PageHits PageSize
def parse_line(line):
    try:
        parts = line.strip().split(' ')
        return (parts[0], parts[1], int(parts[2]), int(parts[3]))
    except:
        return None

parsed_rdd = rdd.map(parse_line).filter(lambda x: x is not None)  # filter out invalid lines
parsed_rdd.persist() # cache the parsed RDD to use it later

# Q1: page size stats
start_time = time.time()

sizes_rdd = parsed_rdd.map(lambda line: line[3]).filter(lambda x: x >= 0)
min_size = sizes_rdd.min()
max_size = sizes_rdd.max()
avg_size = sizes_rdd.mean()

print("Q1 \t min size: ", min_size, "\t Max size: ", max_size, "\t Avg size:", avg_size)
print("Q1 \t total time in seconds:", time.time() - start_time, "s")


# Q2: titles starting with 'The' & not 'en'
start_time = time.time()

the_titles_rdd = parsed_rdd.filter(lambda line: line[1].startswith("The"))
total_the = the_titles_rdd.count()
non_en_the = the_titles_rdd.filter(lambda line: line[0] != "en").count()

print('\n\n')
print("Q2 \t total titles starting with 'The': ", total_the)
print("Q2 \t total titles starting with 'The' & not 'en': ", non_en_the)
print("Q2 \t total time in seconds:", time.time() - start_time, "s")


# Q3: Unique terms in page_title
start_time = time.time()

def normalize_term(term):
    # lowercase, remove non-alphanumeric characters
    return re.sub(r'\W+', '', term.lower())

unique_terms = (parsed_rdd
    .flatMap(lambda line: line[1].split('_'))  # split title by '_', flatMap to return a flattened list of terms
    .map(normalize_term)  # normalize each term
    .filter(lambda term: term != '')  # filter out empty terms
    .distinct()  # get unique terms only
)

print("\n\n")
print("Q3 \t total count of unique terms: ", unique_terms.count())
print("Q3 \t unique terms: ", unique_terms.take(5))
print("Q3 \t total time in seconds:", time.time() - start_time, "s")

# Q4: extract title and count occurrences
start_time = time.time()

title_counts = (parsed_rdd
                .map(lambda line: (line[1], 1))  # map line to (title, 1)
                .reduceByKey(lambda a, b: a + b)) # a and b are the values of the same title

print("\n\n")
print("Q4 \t RDD with title counts: ", title_counts.take(5))
print("Q4 \t total time in seconds:", time.time() - start_time, "s")

# Q5: Group by title
start_time = time.time()

# (title, (project, hits, size))
combined_by_title = (parsed_rdd
    .map(lambda x: (x[1], [(x[0], x[2], x[3])]))  # value is a list to allow easy merge
    .reduceByKey(lambda a, b: a + b) # combine lists of tuples
)

print("\n\n")
print("Q5 \t RDD with combined by title: ", combined_by_title.take(5))
print("Q5 \t total time in seconds:", time.time() - start_time, "s")

combined_by_title.saveAsTextFile("output/combined_by_title")
