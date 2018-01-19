#cd C:\Users\lenovo\Desktop\luminol-master\luminol-master\src\luminol
#ipython --pylab

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import csv
import datetime
import time
from luminol.anomaly_detector import AnomalyDetector
in_path = 'K://Algorithm_study_insuyan/result/result_1121/data/'

my_detector = AnomalyDetector(in_path + '172.16.0.139.csv')
score = my_detector.get_all_scores()

with open(in_path+"172.16.0.139_score2.csv", "w") as csvfile:
    writer = csv.writer(csvfile)
    for timestamp, value in score.iteritems():
        t_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))
        writer.writerow([t_str, value])

csvfile.close()


# pathname_r="C://Users/lenovo/Desktop/luminol-master/luminol-master/src/luminol/172.16.0.139.csv"
data = pd.read_csv(in_path+'172.16.0.139.csv', delimiter=',',names=['kpi_time', 'kpi_value'], index_col=0,parse_dates=True)
# pathname_r="C://Users/lenovo/Desktop/luminol-master/luminol-master/src/luminol/172.16.0.139_score2.csv"
data2 = pd.read_csv(in_path+'172.16.0.139_score2.csv', delimiter=',',names=['kpi_time', 'kpi_value'], index_col=0,parse_dates=True)
fig=plt.figure(1)
ax=fig.add_subplot(1,1,1)
data.plot(ax=ax,style='k.-')
data2.plot(ax=ax,style='b-')
plt.show()

anomly = data[data2['kpi_value']>100]
fig=plt.figure(2)
ax=fig.add_subplot(1,1,1)
data.plot(ax=ax,style='k.-')
anomly.plot(ax=ax,style='r.')
plt.show()








