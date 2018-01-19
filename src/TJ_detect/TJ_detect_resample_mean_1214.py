# coding=utf-8

import pandas as pd
import numpy as np
import time
import datetime
import os
from luminol.anomaly_detector import AnomalyDetector
import matplotlib.pyplot as plt

FORMAT1 = '%Y-%m-%d %H:%M:%S'
read_path = 'K://Algorithm_study_insuyan/data/TJMetrics/'
group_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/group_data/'
count_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/count/'
# score_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/detect_score/'
# detect_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/detect_data/'
# figure_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/score_figure/'

# resample_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/resample_1/resample_data/'
# resample_score_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/resample_1/resample_score/'
# resample_figure_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/resample_1/resample_figure/'

resample_mean_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/resample_1/resample_mean_data/'
resample_mean_score_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/resample_1/resample_mean_score/'
resample_mean_figure_path = 'K://Algorithm_study_insuyan/data/TJ_result_2/resample_1/resample_mean_figure/'


def listdir(path, list_name, file_name):  # 传入存储的list
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
        temp.append([timestamp_to_datetime(timestamp / 1000), value])
    temp_df = pd.DataFrame(temp, columns=['kpi_time', 'kpi_value'])
    return temp_df


file_list = []
filename_list = []
filename = []
index_name = []
listdir(read_path, file_list, filename_list)

metrics = []
data_dic = {}
for i in range(len(file_list)):
    '''从多个时间点文件中读取数据，将每个大指标多时间点数据合并起来，使用字典形式保存，供下一步调用'''
    data = pd.read_csv(file_list[i], names=['ip', 'metrics', 'type', 'timestamp', 'value'])
    data_df = pd.DataFrame(data)
    current_metrics = filename_list[i].split('_2017')[0]
    if current_metrics in metrics:
        print 1
        data_dic[current_metrics] = pd.concat([data_dic[current_metrics], data_df])
    else:
        print 2
        metrics.append(current_metrics)
        data_dic[current_metrics] = data_df

for i in range(len(metrics)):
    # 分别对每个大指标数据处理，先提取处理存放到temp变量
    temp = data_dic[metrics[i]]
    temp.index = np.arange(len(data_dic[metrics[i]]))
    temp = data_dic[metrics[i]]

    # 使用聚组的方式，将取出ip，metrics，type三个指标并统计每组的样本数，作为下一步提取具体ip，metrics，type具体的时间序列数据
    grouped = temp.groupby(['ip', 'metrics', 'type'])
    group_data = grouped.count()
    print group_data
    group_index = pd.DataFrame(list(zip(group_data.index, group_data.values)))
    # # 将group 后的 ip：pon 只取ip，并将数据存取到本地路径
    # group_index.ix[:, 0] = group_index.ix[:, 0]
    group_index.to_csv(group_path + metrics[i] + '_group.csv', index=False, header=False)
    group_data.to_csv(count_path + metrics[i] + '_count.csv')
    # for j in range(len(group_index)):
    # for j in range(len(group_index)):
    #     # 取出一个大指标下面的一个类小指标（由ip，metrics，type定义 ）
    #     data = temp[(temp['ip'] == group_index.ix[j, 0]) &
    #                 (temp['metrics'] == group_index.ix[j, 1]) &
    #                 (temp['type'] == group_index.ix[j, 2])]
    #     # 由于存在/dev/ 类似的字段，后面存储数据应用这个组合文件名会报错，这里将组合后的字符串替换'/' --> '_'
    #     data_flag = group_index.ix[j, 0].split(':')[0] + '_' + group_index.ix[j, 1] + '_' + group_index.ix[j, 2]
    #     data_flag = data_flag.replace('/', '_')
    #     print '-' * 30 + str(i) + '_' * 30 + str(j) + '*' * 30
    #     print data_flag, type(data_flag)
    #     # 根据时间戳排序，并重置索引
    #     detect_data = data.ix[:, 3:5].sort_values(['timestamp'])
    #     detect_data.index = np.arange(len(detect_data))
    #
    #     detect_data['timestamp'] = detect_data['timestamp'].apply(timestamp_to_datetime)
    #
    #     detect_series = detect_data['value']
    #     detect_series.index = detect_data['timestamp'].apply(lambda x: datetime.datetime.strptime(x, FORMAT1))
    #
    #     detect_min_series = detect_series.resample('1T', how='mean')
    #     rolling_mean_data = pd.rolling_mean(detect_min_series, 3)
    #     rolling_mean_data[rolling_mean_data.isnull()] = detect_min_series[rolling_mean_data.isnull()]
    #
    #     # rolling_median_data = pd.rolling_median(detect_min_series, 3)
    #     # rolling_median_data[rolling_median_data.isnull()] = detect_min_series[rolling_median_data.isnull()]
    #
    #     series_to_csv(resample_mean_path + data_flag + '_rolling_mean.csv', rolling_mean_data)
    #
    #     detector = AnomalyDetector(resample_mean_path + data_flag + '_rolling_mean.csv')
    #     score = detector.get_all_scores()
    #     print score
    #     score_df = score_to_df(score)
    #     score_df.to_csv(resample_mean_score_path + str(data_flag) + '__rolling_mean_score.csv', index=False, header=False)
    #     print score_df.head()
    #     print detect_series.head()
    #
    #     # prepare series data:detect, score
    #
    #     score_series = score_df['kpi_value']
    #     score_series.index = score_df['kpi_time'].apply(lambda x: datetime.datetime.strptime(x, FORMAT1))
    #
    #     # plot figure
    #     fig, axes = plt.subplots(2, 1)
    #     axes[0].plot(detect_series)
    #     axes[0].set_title(str(data_flag))
    #     axes[0].legend(['rolling mean series'])
    #     axes[1].plot(score_series, color='r', linestyle='-')
    #     axes[1].legend(['score series'])
    #     plt.savefig(resample_mean_figure_path + str(data_flag) + '_rolling_mean.png', dpi=300)
    #     # plt.show()
    #     plt.close()


