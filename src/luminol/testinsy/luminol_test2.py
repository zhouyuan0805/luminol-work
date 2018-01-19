#coding=utf-8

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import csv
import datetime
import time
from luminol.anomaly_detector import AnomalyDetector


read_path = 'K://Algorithm_study_insuyan/data/DATA_TEST_LML/data/'
write_path = 'K://Algorithm_study_insuyan/data/DATA_TEST_LML/result/'

filename = ['10.17.1.1_Available_memory', '10.17.1.1_Context switches per second',
            '10.17.1.1_CPU user time', '10.17.1.1_Incoming network traffic on bond6',
            '10.17.1.1_Outgoing network traffic on bond6', '10.17.1.1_Processor load (1 min average per core)']


def timestamp_to_datetime(x):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(x))

read_data = pd.read_csv(read_path + filename[0]+'.csv', delimiter='\t')

detect_df = pd.DataFrame(read_data)
detect_df['clock'] = detect_df['clock'].apply(timestamp_to_datetime)
detect_df.to_csv(write_path+'/detect_data/'+filename[0]+'_detect.csv', index=False, header=False)

my_detector = AnomalyDetector(write_path+'/detect_data/'+filename[0]+'_detect.csv')
score = my_detector.get_all_scores()

# with open(write_path+'/score_data/'+filename[0]+'_score.csv', "w") as csvfile:
#     writer = csv.writer(csvfile)
#     for timestamp, value in score.iteritems():
#         t_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))
#         writer.writerow([t_str, value])
# csvfile.close()

score_df = pd.DataFrame(score)
score_df.columns = ['kpi_time', 'kpi_value']
score_df['kpi_time'] = score_df['kpi_time'].apply(lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x / 1000)))
result_score = score_df['kpi_value']
result_score.index = score_df['kpi_time']
# result_score = pd.read_csv(write_path+'/score_data/'+filename[0]+'_score.csv', delimiter=',', names=['kpi_time', 'kpi_value'], index_col=0,parse_dates=True)
detect_series = detect_df['value']
detect_series.index = detect_df['clock']

fig, axes = plt.subplots(2, 1)
print detect_series
axes[0].plot(detect_series, 'k.-')
axes[0].lenged = ['original series']
# detect_series.plot(ax=axes[0], style='k.-')

# axes[1].plot(result_score, 'b.-')
# axes[1].lenged = ['score series']

result_score.plot(ax=axes[1], style='b-')
axes[1].lenged = ['score series']
plt.title(filename[0]+'detect_compare_min200_max200')
# plt.savefig(write_path+'/figure/'+filename[0]+'_min200_max200.png', dpi=300)
plt.show()



# score_df = pd.DataFrame(score)
#
# print score_df.head()

#
#
# my_detector = AnomalyDetector(read_path + filename[0]+'.csv')
# score = my_detector.get_all_scores()
#



#
# # pathname_r="C://Users/lenovo/Desktop/luminol-master/luminol-master/src/luminol/172.16.0.139.csv"
# data = pd.read_csv(read_path+filename[0] + '.csv', delimiter=',',names=['kpi_time', 'kpi_value'], index_col=0, parse_dates=True)
# # pathname_r="C://Users/lenovo/Desktop/luminol-master/luminol-master/src/luminol/172.16.0.139_score2.csv"
# data2 = pd.read_csv(read_path + filename[0] + '_score.csv', delimiter=',', names=['kpi_time', 'kpi_value'], index_col=0,parse_dates=True)
# fig=plt.figure(1)
# ax=fig.add_subplot(1,1,1)
# data.plot(ax=ax,style='k.-')
# data2.plot(ax=ax,style='b-')
# plt.show()
#
# anomly = data[data2['kpi_value'] > 100]
# fig=plt.figure(2)
# ax=fig.add_subplot(1,1,1)
# data.plot(ax=ax,style='k.-')
# anomly.plot(ax=ax,style='r.')
# plt.show()
#
#






