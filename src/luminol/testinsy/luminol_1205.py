# -*- coding: utf-8 -*-
import os
import pandas as pd
import datetime
import time
import numpy as np
import matplotlib.pyplot as plt
import csv
from luminol.anomaly_detector import AnomalyDetector



def listdir(path, list_name, file_name):  #传入存储的list
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isdir(file_path):
                  listdir(file_path, list_name)
        else:
            list_name.append(file_path)
            file_name.append(file)


def timestamp_to_datetime(x):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x))

def read_csv_to_df(file_path, delimiter='\t', flag=1):
    def timestamp_to_datetime(x):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x))

    if (flag == 1):
        csv_data = pd.read_csv(file_path, delimiter=delimiter)
        data_df = pd.DataFrame(csv_data)
        data_df.ix[:, 0] = data_df.ix[:, 0].apply(timestamp_to_datetime)
        return data_df
    else:

        data_df = pd.read_csv(file_path, delimiter=delimiter,
                               names=['kpi_time', 'kpi_value'], index_col=0, parse_dates=True)
        return data_df

def df_to_series(data_df):
    series_tmp = data_df.ix[:, 1]
    series_tmp.index = data_df.ix[:, 0].apply(lambda x: datetime.datetime.strptime(x, FORMAT1))
    return series_tmp

def series_to_df(data_series):
    data_frame= pd.DataFrame(list(zip(data_series.index, data_series.values)))
    data_frame.columns = ['kpi_time', 'kpi_value']
    return data_frame

def series_to_csv(write_path, data_series):
    df = series_to_df(data_series)
    df.to_csv(write_path, index=False, header=False)

def score_to_df(score_data):
    temp =[]
    for timestamp, value in score.iteritems():
        temp.append([timestamp_to_datetime(timestamp/1000), value])
    temp_df = pd.DataFrame(temp, columns=['kpi_time', 'kpi_value'])
    return temp_df

FORMAT1 = '%Y-%m-%d %H:%M:%S'
read_path = 'K://Algorithm_study_insuyan/data/luminol_ip30/data/'
sample_path = 'K://Algorithm_study_insuyan/data/luminol_ip30/sample_data/'
figure_path = 'K://Algorithm_study_insuyan/data/luminol_ip30/figure/'
score_path = 'K://Algorithm_study_insuyan/data/luminol_ip30/score/'

file_list = []
filename_list = []
filename = []
index_name = []
listdir(read_path, file_list, filename_list)
for i in filename_list:
    filename.append(os.path.splitext(i)[0])
    index_name.append(os.path.splitext(i)[0].split('_')[1])

for i in range(len(file_list)):
    original_df = read_csv_to_df(file_list[i],flag=1)
    original_series = df_to_series(original_df)

    sample_series = original_series.resample('5min',how='mean',closed='left')
    # print sample_series['2017-11-18']
    series_to_csv(sample_path + '02/'+index_name[i] + '_5min_sample.csv', sample_series['2017-11-02'])
    print index_name[i]
    my_detector = AnomalyDetector(sample_path + '02/' + index_name[i] + '_5min_sample.csv')
    score = my_detector.get_all_scores()
    score_series = df_to_series(score_to_df(score))
    series_to_csv(score_path + '02/' + filename[i] + '.csv', score_series)

#plot figure
    fig, axes = plt.subplots(2, 1)
    axes[0].plot(sample_series['2017-11-02'])
    axes[0].set_title(index_name[i]+'5 min resample series and score D:1102')
    axes[0].legend(['sample series'])
    axes[1].plot(score_series, color='r', linestyle='-')
    axes[1].legend(['score series'])
    plt.savefig(figure_path + '020/' + filename[i] + '.png', dpi=300)
    plt.close()

# for i in range(len(file_list)):
#     original_df = read_csv_to_df(file_list[i],flag=1)
#     original_series = df_to_series(original_df)
#
#     sample_series = original_series.resample('5min',how='mean',closed='left')
#     # print sample_series['2017-11-18']
#     series_to_csv(sample_path + '02/'+index_name[i] + '_5min_sample.csv', sample_series['2017-11-02'])
#     print index_name[i]
#     my_detector = AnomalyDetector(sample_path + '02/' + index_name[i] + '_5min_sample.csv')
#     score = my_detector.get_all_scores()
#     score_series = df_to_series(score_to_df(score))
#     series_to_csv(score_path + '02/' + filename[i] + '.csv', score_series)
#
# #plot figure
#     fig, axes = plt.subplots(2, 1)
#     axes[0].plot(sample_series['2017-11-02'])
#     axes[0].set_title(index_name[i]+'5 min resample series and score D:1102')
#     axes[0].legend(['sample series'])
#     axes[1].plot(score_series, color='r', linestyle='-')
#     axes[1].legend(['score series'])
#     plt.savefig(figure_path + '020/' + filename[i] + '.png', dpi=300)
#     plt.close()
