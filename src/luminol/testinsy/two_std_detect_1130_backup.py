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
                  listdir(file_path, list_name)
        else:
            list_name.append(file_path)
            file_name.append(file)

# in_path = 'K://Algorithm_study_insuyan/data/quater_data/mpp/incoming/'
# result_path = 'K://Algorithm_study_insuyan/data/quater_data/mpp/incoming_result/'

# in_path = 'K://Algorithm_study_insuyan/data/quater_data/mpp/outgoing/'
# result_path = 'K://Algorithm_study_insuyan/data/quater_data/mpp/outgoing_result/'

in_path = 'K://Algorithm_study_insuyan/data/quater_data/hadoop/incoming/'
result_path = 'K://Algorithm_study_insuyan/data/quater_data/hadoop/incoming_result/'

# in_path = 'K://Algorithm_study_insuyan/data/quater_data/hadoop/outgoing/'
# result_path = 'K://Algorithm_study_insuyan/data/quater_data/hadoop/outgoing_result/'



FORMAT1 = '%Y-%m-%d %H:%M:%S'

filepath_list = []
filename_list = []
filename = []
ip_list = []
listdir(in_path, filepath_list, filename_list)
for i in filename_list:
    filename.append(os.path.splitext(i)[0])
    ip_list.append(i.split('_')[0])
# print ip_list
summary_series = []
# for i in range(len(ip_list)):
for i in range(len(filepath_list)):
    test_data = pd.read_csv(filepath_list[i], delimiter='\t')
    test_df = pd.DataFrame(test_data)
    test_df['clock'] = test_df['clock'].apply(timestamp_to_datetime)
    test_series = test_df['value']
    test_series.index = test_df['clock'].apply(lambda x: datetime.datetime.strptime(x,FORMAT1))

    five_minute_data = test_series.resample('5T', how='mean', closed='left')
    five_minute_data2 = pd.DataFrame(list(five_minute_data.values))
    five_minute_data2.columns = ['kpi_value']
    five_minute_data2.index = five_minute_data.index
    # five_minute_data2['ip'] = ip_list[i]
    print ip_list[i]
    print '*'*10 + str(i)
    # print five_minute_data2
    # print type(five_minute_data2)
    if len(summary_series) == 0:
        summary_series = five_minute_data2
    else:
        # summary_series = pd.concat([summary_series,five_minute_data2])
        summary_series = pd.concat([summary_series,five_minute_data2],axis=1)
# print summary_series

#compute mean and std for each time(5 min)
mean_series = summary_series.mean(1)
std_series = summary_series.std(1)

#get upper and down for each time(5 min)
upper_series = mean_series + 2 * std_series
down_series = mean_series - 2 * std_series
upper_series.to_csv(result_path + 'upper_series_with_2std.csv')
down_series.to_csv(result_path + 'down_series_with_2std.csv')

# len(filepath_list)
upper_data = pd.read_csv(result_path + 'upper_series_with_2std.csv',names=['kpi_time','upper'],parse_dates=True)
down_data = pd.read_csv(result_path + 'down_series_with_2std.csv',names=['kpi_time','down'],parse_dates=True)
upper_df = pd.DataFrame(upper_data)
down_df = pd.DataFrame(down_data)


stand_data = down_df.merge(upper_df, on=['kpi_time'])
stand_data['kpi_time'] = stand_data['kpi_time'].apply(lambda x: datetime.datetime.strptime(x, FORMAT1))

for i in range(len(filepath_list)):
    test_data = pd.read_csv(filepath_list[i], delimiter='\t')
    test_df = pd.DataFrame(test_data)
    test_df.columns = ['kpi_time','kpi_value']

    test_df['kpi_time'] = test_df['kpi_time'].apply(timestamp_to_datetime)
    # test_df['kpi_time'] = test_df['kpi_time'].apply(lambda x: datetime.datetime.strptime(x,FORMAT1))

    test_series = test_df['kpi_value']
    test_series.index = test_df['kpi_time'].apply(lambda x: datetime.datetime.strptime(x,FORMAT1))

    five_minute_data = test_series.resample('5T', how='mean', closed='left')
    five_minute_data2 = pd.DataFrame(list(zip(five_minute_data.index, five_minute_data.values)))
    five_minute_data2.columns = ['kpi_time', 'kpi_value']
    # five_minute_data2['kpi_time'] = five_minute_data2['kpi_time'].apply(lambda x: datetime.datetime.strftime(x,FORMAT1))

    # print five_minute_data2
    # print stand_data
    # print '*'*100
    #
    # print type(five_minute_data2.ix[1, 'kpi_time'])
    # print type(stand_data.ix[1, 'kpi_time'])

    all_data = pd.merge(five_minute_data2, stand_data, on=['kpi_time'])
    all_data = all_data.dropna(how='any')
    all_data['lable'] = 0
    all_data['lable'][all_data['kpi_value'] > all_data['upper']] = 1
    all_data['lable'][all_data['kpi_value'] < all_data['down']] = -1
    all_data.ix[:,[0,1,2,3,4]].to_csv(result_path + filename[i] + '_two_std.csv',index=False)
    print '*' * 30 + str(ip_list[i]) + '*' * 30 + 'remain' + str(len(filepath_list) - i - 1) + '*' * 30


#     # five_minute_data2.index = five_minute_data.index
#     five_minute_data['label'] = 0
#     # print five_minute_data2
#     # print five_minute_data2
# #
# print test_df
# all_data = pd.merge(test_df, stand_data, on=['kpi_time'])
# print all_data
# print all_data.isnull()

#     print len(five_minute_data),len(upper_series),len(down_series)
#
#     five_minute_data = five_minute_data.dropna(axis=0, how='any')
#     # print five_minute_data2.head()
#     five_minute_data.to_csv(result_path + filename[i] + '_two_std.csv')
#     print '*' * 30 + str(ip_list[i]) + '*' * 30 + 'remain' + str(len(filepath_list) - i - 1) + '*' * 30

# upper_series = upper_series.dropna(axis=0, how='any')
# down_series = down_series.dropna(axis=0, how='any')





# for j in range(len(upper_series)):
    #     print float(five_minute_data.ix[j])
    #     print float(upper_series.ix[j])
    #     if float(five_minute_data.ix[j:,1]) > float(upper_series.ix[j]):
    #         five_minute_data.ix[j,1] = 1
    #         # print 1
    #     elif float(five_minute_data.ix[j]) < float(down_series.ix[j]):
    #         five_minute_data.ix[j,1] = -1
            # print -1
#     five_minute_data = five_minute_data.dropna(axis=0, how='any')
#     # print five_minute_data2.head()
#     five_minute_data.to_csv(result_path + filename[i] + '_two_std.csv')
#     print '*'*30 + str(ip_list[i]) + '*'*30 + 'remain' + str(len(filepath_list)-i-1)+'*'*30
#
#
# # upper_series = upper_series.dropna(axis=0, how='any')
# # down_series = down_series.dropna(axis=0, how='any')
# upper_series.to_csv(result_path + 'upper_series_with_2std.csv')
# down_series.to_csv(result_path + 'down_series_with_2std.csv')

    # print j, five_minute_data.index[j], ip_list[i]
            # print five_minute_data[five_minute_data.index[j]]
