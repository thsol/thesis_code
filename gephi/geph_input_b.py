import pandas as pd
from pyspark.sql.functions import udf, col
import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import hour, mean
from pyspark.sql.types import *

# This notebook combines a list of doc ids and topic ids for graph analysis

sc = pyspark.SparkContext()

path_x = '/Users/tiegsti.solomon/Downloads'
path_y = '/Users/tiegsti.solomon'

nodes = sc.textFile(path_y+"/nodes_recovered.csv")
doc_topics = sc.textFile(path_x+"/documents_topics.csv")

# remove header train
rdd_nodes_head = nodes.filter(lambda l: "id" in l)
rdd_nodes = nodes.subtract(rdd_nodes_head)

# remove header test
rdd_topics_head = doc_topics.filter(lambda l: "doc_id" in l)
rdd_topics = doc_topics.subtract(rdd_topics_head)

rdd_n = rdd_nodes.map(lambda a: a.split(","))
rdd_t = rdd_topics.map(lambda a: a.split(","))

sqlContext = SQLContext(sc)
nodes_ = sqlContext.createDataFrame(rdd_n) # schema err
topics_ = sqlContext.createDataFrame(rdd_t)
topics_.show(5)
nodes_.show(5)

nodes_dfp = nodes_.toPandas()
topics_dfp = topics_.toPandas()

def convert_id_list(doc_ids):
    return ' '.join(d.split(':')[1] for d in doc_ids.split(' '))

# nodes_dfp = nodes_dfp['_1'].unique()
doc_ids = pd.DataFrame(nodes_dfp, columns = ['_1','_2','_3','_4'])
doc_ids = doc_ids.rename(index=str, columns={'_1':'_1_1'})
topic_ids = pd.DataFrame(topics_dfp, columns = ['_1','_2'])

#merged_dfp = doc_ids.set_index('_1').join(topic_ids.set_index('_1'))

merged_dfp2 = pd.merge(doc_ids, topic_ids,left_on="_1_1",right_on="_1", how='left')
