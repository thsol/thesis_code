# General code generate graphs (Degree vs Betweenness Example)
import numpy as np
import pandas as pd
# import pylon
import random
import csv
import matplotlib.pyplot as plt


import pandas as pd

# General code generate graphs (Degree vs Betweenness Example)
#file_name = ('/Users/tiegsti.solomon/Downloads/log_input_weighted.csv')
#file_name = ('/Users/tiegsti.solomon/donwloads/_input_unweighted.csv')


# df =  pd.read_csv(file_name)
# g = df.groupby("degree")
# g = g["degree"].count()


#g.to_csv("/Users/tiegsti.solomon/Downloads/_weighted.csv", sep=',')
#g.to_csv("/Users/tiegsti.solomon/Downloads/_unweighted.csv", sep=',')

# output to a file

#np.set_printoptions(suppress=True)

#file_name2 = ('/Users/tiegsti.solomon/Downloads/_weighted.csv')
file_name2 = ('/Users/tiegsti.solomon/Downloads/_weighted.csv')

csv = np.genfromtxt (file_name2, delimiter=",", dtype=float)
y =  csv[1:,1]
x = csv[1:,0]


# set limits for the axes
plt.gca().set_ylim([0.9,1000])
plt.gca().set_xlim([0.9,1000])
 
# log-log plot
plt.gca().set_xscale("log")
plt.gca().set_yscale("log")

# label axis
plt.xlabel('Degree (k)', fontsize=10)
plt.ylabel('P (k)', fontsize=10)

plt.plot(x, y, 'o')
plt.show()