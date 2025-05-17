#%%
# !pip install pyspark
#%%
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Assignment2 Spark loops").getOrCreate()
sc = spark.sparkContext

#%%
# from google.colab import drive
# drive.mount('/content/drive')
#
# rdd = sc.textFile("/content/drive/MyDrive/pagecounts-20160101-000000_parsed.out")
# print(rdd.first())  # just to make sure the file is read


rdd = sc.textFile("work/FCAI/BigData/spark-ass2/pagecounts-20160101-000000_parsed.out")
print(rdd.first())  # just to make sure the file is read
#%%
import time
import re

def parse_line(line):
    try:
        parts = line.strip().split(' ')
        return (parts[0], parts[1], int(parts[2]), int(parts[3]))
    except:
        return None

parsed_rdd = rdd.map(parse_line).filter(lambda x: x is not None)
parsed_rdd.persist()


# Q1: page size stats
start_time = time.time()

sizes = parsed_rdd.map(lambda line: line[3]).filter(lambda x: x >= 0).collect()

min_size = min(sizes)
max_size = max(sizes)
avg_size = sum(sizes) / len(sizes) if sizes else 0

print("Q1 (Loops) \t min size:", min_size, "\t Max size:", max_size, "\t Avg size:", avg_size)
print("Q1 (Loops) \t total time in seconds:", time.time() - start_time, "s")


# Q2: titles starting with 'The' & not 'en'
start_time = time.time()

lines = parsed_rdd.collect()

the_titles = [line for line in lines if line[1].startswith("The")]
non_en_the = [line for line in the_titles if line[0] != "en"]

print("\n\n")
print("Q2 (Loops) \t total titles starting with 'The':", len(the_titles))
print("Q2 (Loops) \t total non-English titles starting with 'The':", len(non_en_the))
print("Q2 (Loops) \t total time in seconds:", time.time() - start_time, "s")


# Q3: Unique terms in page_title
start_time = time.time()

def normalize_term(term):
    return re.sub(r'\W+', '', term.lower())

titles = parsed_rdd.map(lambda x: x[1]).collect()

terms_set = set()
for title in titles:
    terms = title.split('_')
    for term in terms:
        normalized = normalize_term(term)
        if normalized:
            terms_set.add(normalized)

print("\n\n")
print("Q3 (Loops) \t total unique normalized terms:", len(terms_set))
print("Q3 (Loops) \t sample terms:", list(terms_set)[:5])
print("Q3 (Loops) \t total time in seconds:", time.time() - start_time, "s")


# Q4: Count titles
start_time = time.time()

titles = parsed_rdd.map(lambda x: x[1]).collect()

title_count_dict = {}
for title in titles:
    title_count_dict[title] = title_count_dict.get(title, 0) + 1

print("\n\n")
print("Q4 (Loops) \t title count sample:", list(title_count_dict.items())[:5])
print("Q4 (Loops) \t total time in seconds:", time.time() - start_time, "s")



# Q5: Group lines by title
start_time = time.time()

lines = parsed_rdd.collect()

combined_dict = {}
for line in lines:
    project, title, hits, size = line
    if title not in combined_dict:
        combined_dict[title] = []
    combined_dict[title].append((project, hits, size))

print("\n\n")
print("Q5 (Loops) \t sample combined titles:")
for k, v in list(combined_dict.items())[:5]:
    print(f"{k} -> {v}")

print("Q5 (Loops) \t total time in seconds:", time.time() - start_time, "s")

