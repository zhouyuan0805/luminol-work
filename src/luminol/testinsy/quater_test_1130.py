# -*- coding: utf-8 -*-
import os
import pandas as pd
import datetime
import time
import numpy as np
import matplotlib.pyplot as plt
import csv
from luminol.anomaly_detector import AnomalyDetector
import luminol.utils

def timestamp_to_datetime(x):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(x))

def listdir(path, list_name, file_name):  #传入存储的list
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isdir(file_path):
                  listdir(file_path, list_name, file_name)
        else:
            list_name.append(file_path)
            file_name.append(file)

in_path = 'K://Algorithm_study_insuyan/data/quater_data/mpp/incoming_data/'
# ewm_path = 'K://Algorithm_study_insuyan/data/luminol/mpp_ewm/'
# ewm_score_path = 'K://Algorithm_study_insuyan/data/luminol/mpp_ewm_score/'
# figure_ewm = 'K://Algorithm_study_insuyan/data/luminol/figure_ewm/'

FORMAT1 = '%Y-%m-%d %H:%M:%S'

filepath_list = []
filename_list = []
filename = []
ip_list = []
listdir(in_path, filepath_list, filename_list)
for i in filename_list:
    filename.append(os.path.splitext(i)[0])
    ip_list.append(i.split('_')[0])
print ip_list
summary_series = []
# for i in range(len(ip_list)):
for i in range(2):
    test_data = pd.read_csv(filepath_list[i], delimiter='\t')
    test_df = pd.DataFrame(test_data)
    test_df['clock'] = test_df['clock'].apply(timestamp_to_datetime)
    test_series = test_df['value']
    test_series.index = test_df['clock'].apply(lambda x: datetime.datetime.strptime(x,FORMAT1))

    five_minute_data = test_series.resample('5T', how='mean', closed='left')
    five_minute_data2 = pd.DataFrame(list(zip(five_minute_data.index,five_minute_data.values)))
    five_minute_data2.columns = ['kpi_time','kpi_value']
    five_minute_data2['ip'] = ip_list[i]
    print ip_list[i]
    print '*'*10 + str(i)
    # print five_minute_data2
    # print type(five_minute_data2)
    if len(summary_series) == 0:
        summary_series = five_minute_data2
    else:
        summary_series = pd.concat([summary_series,five_minute_data2])
        # summary_series = summary_series.merge(five_minute_data2,on='kpi_time')
summary_series.index = np.arange(len(summary_series))
# print summary_series.groupby(['ip']).size()
summary_series['group_sort']=summary_series['kpi_value'].groupby(summary_series['kpi_time']).rank(ascending=0,method='dense')
# print summary_series.groupby(['group_sort']).size()
print summary_series[summary_series['group_sort'].isnull().values==True]
print summary_series[summary_series['ip']=='10.17.1.46']
# print summary_series.groupby(['ip']).size()
# max_df = summary_series[summary_series['group_sort'] == 1.0].sort_values('kpi_time')
# median_df = summary_series[summary_series['group_sort'] == 2.0].sort_values('kpi_time')
# min_df = summary_series[summary_series['group_sort'] == 3.0].sort_values('kpi_time')
#
# max_series = max_df['kpi_value']
# max_series.index = max_df['kpi_time']
#
# median_series = median_df['kpi_value']
# median_series.index = median_df['kpi_time']
#
# min_series = min_df['kpi_value']
# min_series.index = min_df['kpi_time']
# print max_series
# print median_series
# print min_series
# max_series['2017-11-20'].plot()
# median_series['2017-11-20'].plot()
# min_series['2017-11-20'].plot()
# plt.legend(['max','median','min'])
# plt.show()
# series_max = summary_series['kpi_value']
# series_max.index = series_max


# print summary_series[summary_series['ip']=='10.17.1.46']
#     #
    # print test_series
    # print five_minute_data