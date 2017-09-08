import numpy as np
import pandas as pd
# import pylon
import random
import csv


# features as input
input_file = "/Users/tiegsti.solomon/Downloads/content_reco_features_5000.csv"

# comma delimited is the default, set data type to unicode
df = pd.read_csv(input_file, header = 0, dtype="unicode")
# put the original column names in a python list

original_headers = list(df.columns.values)

# # create a numpy array
numpy_array = df.as_matrix()

# # delete records not needed
strip_numpy_array = np.delete(numpy_array, [3,4,6,7,9,10,11,12,13,14,15,16], axis=1)

# remove records of non-clicked activity by using boolean to check each element for where there's a "0"
# clk_conversions = strip_numpy_array[np.logical_not(strip_numpy_array[:,-1] == "0")]  # uncomment to include just click data

node = np.delete(strip_numpy_array, [0, 1], axis=1)
edge = np.delete(strip_numpy_array, [1, 3, 5], axis=1)

# # [0] uuid, [1] ad_id, [2] document_id, [3] clicked
# # create node array for nodes.csv
# # create edge array for edges.csv
# # empty dataset 
nodes_out = list()
uniq_uuid = list()
edges_out = list()

# # node [0] ad_id, [1] document_id
nodes_out.append(node)

# # edges [0] uuid , [1] ad_id    
# make sure we're only working with uniques
for i in range(len(edge)):
    if edge[i][0] not in uniq_uuid:
        # out -> uuid - ad_id1 , ad_id2, ...
       uniq_uuid.append(edge[i][0]) # -> list of unique uuids


values = set(map(lambda x:x[0], edge))
newlist = [[x,[y[1] for y in edge if y[0]==x]] for x in values]

for i,j in newlist:
    if len(j) > 1:
    # we don't want single ad_ids, these will appear in the nodes.csv file as a node
    # we only want interactions
        combo = [(j[a],j[b]) for a in range(len(j)) for b in range(a+1, len(j))]
        edges_out.append(combo)
        #print(combo)

edges_out = [val for sublist in edges_out for val in sublist]
nodes_out = [val for sublist in nodes_out for val in sublist]

import csv

with open('edges.csv','wb') as out:
    csv_out=csv.writer(out)
    csv_out.writerow(['Source','Target'])
    for row in edges_out:
        csv_out.writerow(row)

with open('nodes.csv','wb') as out:
    csv_out=csv.writer(out)
    csv_out.writerow(['Id','Geo','Label'])
    for row in nodes_out:
        csv_out.writerow(row)

# save new + modified dataset into csv 
#np.savetxt("nodes.csv", nodes_out, fmt="%s", delimiter="\' \'", header="Id, Label, comments='')
#np.savetxt("edges.csv", edges_out, fmt="%s", delimiter=",", header="Source,Target", comments='')

