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

# in_path = 'K://Algorithm_study_insuyan/data/quater_data_three/mpp/incoming_data/'
# result_path = 'K://Algorithm_study_insuyan/data/quater_data_three/mpp/incoming_result/'

# in_path = 'K://Algorithm_study_insuyan/data/quater_data_three/mpp/outgoing_data/'
# result_path = 'K://Algorithm_study_insuyan/data/quater_data_three/mpp/outgoing_result/'

# in_path = 'K://Algorithm_study_insuyan/data/quater_data_three/hadoop/incoming_data/'
# result_path = 'K://Algorithm_study_insuyan/data/quater_data_three/hadoop/incoming_result/'

in_path = 'K://Algorithm_study_insuyan/data/quater_data_three/hadoop/outgoing_data/'
result_path = 'K://Algorithm_study_insuyan/data/quater_data_three/hadoop/outgoing_result/'


FORMAT1 = '%Y-%m-%d %H:%M:%S'

filepath_list = []
filename_list = []
filename = []
ip_list = []
listdir(in_path, filepath_list, filename_list)
for i in filename_list:
    filename.append(os.path.splitext(i)[0])
    ip_list.append(i.split('_')[0])

# param = tuple(ip_list)
# print type(param)
#
# param_dict = {}
# for i in param:
#     param_dict[i] = 'data_series' + str(i.split('.')[3])
#
# print param_dict
#
# def test_dict_param(**args) :
#     for k, v in args.iteritems():
#         temp = v
#
# test_dict_param(**param_dict)

# print ip_list
summary_series = []
# for i in range(len(ip_list)):
tmp = {}
series = {}
sample_series = {}
sample_df = {}
for i in range(len(filepath_list)):
    # locals()[ip_list[i]] = pd.read_csv(filepath_list[i], delimiter='\t')
    # print locals()[ip_list[i]]

    tmp[ip_list[i]] = pd.read_csv(filepath_list[i], delimiter='\t')

    tmp[ip_list[i]].ix[:,0] = tmp[ip_list[i]].ix[:,0].apply(timestamp_to_datetime)
    series[ip_list[i]] = tmp[ip_list[i]].ix[:, 1]
    series[ip_list[i]].index = tmp[ip_list[i]].ix[:,0].apply(lambda x: datetime.datetime.strptime(x,FORMAT1))
    # print series[ip_list[i]]
    sample_series[ip_list[i]] = series[ip_list[i]].resample('5T', how='mean', closed='left')
    print sample_series[ip_list[i]]

    sample_df[ip_list[i]] = pd.DataFrame(list(sample_series[ip_list[i]].values))
    sample_df[ip_list[i]].index = sample_series[ip_list[i]].index
    sample_df[ip_list[i]].columns = [ip_list[i]]

    print ip_list[i]
    print '*'*10 + str(i)
    if len(summary_series) == 0:
        summary_series = sample_df[ip_list[i]]
    else:
        # summary_series = pd.concat([summary_series,five_minute_data2])
        summary_series = pd.concat([summary_series, sample_df[ip_list[i]]], axis=1)
print summary_series
#
# #compute mean and std for each time(5 min)
# mean_series = summary_series.mean(1)
# std_series = summary_series.std(1)
#
# #get upper and down for each time(5 min)
# upper_series = mean_series + 3 * std_series
# down_series = mean_series - 3 * std_series
# upper_series.to_csv(result_path + 'upper_series_with_3std.csv')
# down_series.to_csv(result_path + 'down_series_with_3std.csv')
#
# # len(filepath_list)
# upper_data = pd.read_csv(result_path + 'upper_series_with_3std.csv',names=['kpi_time','upper'],parse_dates=True)
# down_data = pd.read_csv(result_path + 'down_series_with_3std.csv',names=['kpi_time','down'],parse_dates=True)
# upper_df = pd.DataFrame(upper_data)
# down_df = pd.DataFrame(down_data)
#
#
# stand_data = down_df.merge(upper_df, on=['kpi_time'])
# stand_data['kpi_time'] = stand_data['kpi_time'].apply(lambda x: datetime.datetime.strptime(x, FORMAT1))
#
# for i in range(len(filepath_list)):
#     test_data = pd.read_csv(filepath_list[i], delimiter='\t')
#     test_df = pd.DataFrame(test_data)
#     test_df.columns = ['kpi_time','kpi_value']
#
#     test_df['kpi_time'] = test_df['kpi_time'].apply(timestamp_to_datetime)
#     # test_df['kpi_time'] = test_df['kpi_time'].apply(lambda x: datetime.datetime.strptime(x,FORMAT1))
#
#     test_series = test_df['kpi_value']
#     test_series.index = test_df['kpi_time'].apply(lambda x: datetime.datetime.strptime(x, FORMAT1))
#
#     five_minute_data = test_series.resample('5T', how='mean', closed='left')
#     five_minute_data2 = pd.DataFrame(list(zip(five_minute_data.index, five_minute_data.values)))
#     five_minute_data2.columns = ['kpi_time', 'kpi_value']
#
#     all_data = pd.merge(five_minute_data2, stand_data, on=['kpi_time'])
#     all_data = all_data.dropna(how='any')
#     all_data['lable'] = 0
#     all_data['lable'][all_data['kpi_value'] > all_data['upper']] = 1
#     all_data['lable'][all_data['kpi_value'] < all_data['down']] = -1
    # all_data.ix[:,[0,1,2,3,4]].to_csv(result_path + filename[i] + '_three_std.csv',index=False)
    # print '*' * 30 + str(ip_list[i]) + '*' * 30 + 'remain' + str(len(filepath_list) - i - 1) + '*' * 30

