#coding=utf-8

import pandas as pd
import numpy as np
import time
import datetime
import os
from luminol.anomaly_detector import AnomalyDetector
import matplotlib.pyplot as plt

FORMAT1 = '%Y-%m-%d %H:%M:%S'
read_path = 'K://Algorithm_study_insuyan/data/TJMetrics/'
group_path = 'K://Algorithm_study_insuyan/data/TJ result/group_data/'
score_path = 'K://Algorithm_study_insuyan/data/TJ result/detect_score/'
detect_path = 'K://Algorithm_study_insuyan/data/TJ result/detect_data/'
figure_path = 'K://Algorithm_study_insuyan/data/TJ result/score_figure/'

def listdir(path, list_name, file_name):  #传入存储的list
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isdir(file_path):
                  listdir(file_path, list_name)
        else:
            list_name.append(file_path)
            file_name.append(file)

def timestamp_to_datetime(x):
    '''change timestamp to datetime'''
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x))

def series_to_df(data_series):
    data_frame= pd.DataFrame(list(zip(data_series.index, data_series.values)))
    data_frame.columns = ['kpi_time', 'kpi_value']
    return data_frame

def series_to_csv(write_path, data_series):
    df = series_to_df(data_series)
    df.to_csv(write_path, index=False, header=False)

def score_to_df(score_data):
    '''transform luminol detect score as Pandas DataFrame [kpi_time, kpi_value]'''
    temp = []
    for timestamp, value in score.iteritems():
        temp.append([timestamp_to_datetime(timestamp),value])
    temp_df = pd.DataFrame(temp, columns=['kpi_time','kpi_value'])
    return temp_df

file_list = []
filename_list = []
filename = []
index_name = []
listdir(read_path, file_list, filename_list)
print file_list
print filename_list
metrics = []
all_data = []
data_dic = {}
for i in range(len(file_list)):
    data = pd.read_csv(file_list[i],names=['ip', 'metrics', 'type', 'timestamp', 'value'])
    data_df = pd.DataFrame(data)
    current_metrics = filename_list[i].split('_2017')[0]
    if current_metrics in metrics:
        print 1
        # all_data = pd.concat([all_data, data_df])
        # data_dic[current_metrics] = all_data
        data_dic[current_metrics] = pd.concat([data_dic[current_metrics],data_df])
    else:
        metrics.append(current_metrics)
        # all_data = data_df
        data_dic[current_metrics] = data_df
        print 2

for i in range(8, len(metrics)):
    temp = data_dic[metrics[i]]
    temp.index = np.arange(len(data_dic[metrics[i]]))
    temp = data_dic[metrics[i]]
    # print temp[['ip','metrics','type']].unique()
    # print temp['ip'].unique()
    # print temp['metrics'].unique()
    # print temp['type'].unique
    grouped = temp.groupby(['ip', 'metrics', 'type'])
    group_data = grouped.mean()
    group_index = pd.DataFrame(list(group_data.index))
    # group_index.ix[:, 0] = group_index.ix[:, 0].apply(lambda x : x.split(':')[0])
    # group_index.to_csv(group_path + metrics[i] + '_group.csv', index=False,header=False)

    for j in range(len(group_index)):
        data = temp[(temp['ip']==group_index.ix[j,0]) &
                            (temp['metrics']==group_index.ix[j,1]) &
                            (temp['type']==group_index.ix[j,2])]

        data_flag = group_index.ix[j,0].split(':')[0] + '_' + group_index.ix[j,1] + '_' + group_index.ix[j,2]
        data_flag = data_flag.replace('/', '_')
        print '-'* 30 + str(i) + '_'*30 + str(j) + '*'*30
        print data_flag, type(data_flag)
        detect_data = data.ix[:,3:5].sort_values(['timestamp'])
        detect_data.index = np.arange(len(detect_data))

        #prepare dict data for AnomlayDetector Class
        detect_dic = {}
        for m in range(len(detect_data)):
            detect_dic[detect_data.ix[m, 0]] = detect_data.ix[m, 1]

        detector = AnomalyDetector(detect_dic)
        score = detector.get_all_scores()

        score_df = score_to_df(score)
        score_df.to_csv(score_path + str(data_flag) + '_score.csv', index=False, header=False)
        print score_df.head()
        detect_data.ix[:,0] = detect_data.ix[:,0].apply(timestamp_to_datetime)\
                                                 .apply(lambda x: datetime.datetime.strptime(x, FORMAT1))
        print detect_data.head()
        detect_data.to_csv(detect_path + str(data_flag) + '_detect.csv', index=False, header=False)

        #prepare series data:detect, score
        detect_series = detect_data['value']
        detect_series.index = detect_data['timestamp']

        score_series = score_df['kpi_value']
        score_series.index = score_df['kpi_time'].apply(lambda x: datetime.datetime.strptime(x, FORMAT1))

        #plot figure
        fig, axes = plt.subplots(2,1)
        axes[0].plot(detect_series)
        axes[0].set_title(str(data_flag))
        axes[0].legend(['sample series'])
        axes[1].plot(score_series, color='r', linestyle='-')
        axes[1].legend(['score series'])
        plt.savefig(figure_path + str(data_flag) + '_score.png', dpi=300)
        # plt.show()
        plt.close()


