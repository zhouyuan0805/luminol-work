#!/usr/bin/python2
# coding=utf-8

import pandas as pd
import os
import time
import datetime
import matplotlib.pyplot as plt
from matplotlib.dates import DayLocator, HourLocator, DateFormatter
from sklearn import preprocessing


def timestamp_to_datetime(x):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x))


def listdir(path, list_name, file_name):  # 传入存储的list
    for files in os.listdir(path):
        file_path = os.path.join(path, files)
        if os.path.isdir(file_path):
            listdir(file_path, list_name, file_name)
        else:
            list_name.append(file_path)
            file_name.append(files)


def compare_same_period(ts, num):
    # compare with the same period of last day

    min_on_min_df = []
    min_long = []
    names = []
    for i in range(1, num + 1):
        temp = ts / ts.shift(2 * i) - 1
        names.append('compare_' + str(i) + 'min')
        if len(min_on_min_df) == 0:
            min_on_min_df = temp
            min_long =temp
        else:
            min_on_min_df = pd.concat([min_on_min_df, temp], axis=1)
            min_long = pd.concat([min_long, temp])
    min_on_min_df.columns = names
    return min_on_min_df, min_long


def compare_previous_period(ts, num, freq='D'):
    # description: compare with the previous period

    def get_workday_during_period(start, end, week_index=0):
        # get one day of a week,like Monday:0...
        test_date = pd.date_range(start, end)
        test_df = pd.DataFrame(test_date, columns=['date'])
        test_df['week_index'] = test_df['date'].apply(lambda x: x.weekday())
        c_list = list(test_df[test_df['week_index'] == week_index].ix[:, 'date'])
        for c in range(len(c_list)):
            c_list[c] = str(c_list[c]).split(' ')[0]
        return c_list

    start_date = datetime.datetime.strftime(ts.index.min(), '%Y-%m-%d')
    end_date = datetime.datetime.strftime(ts.index.max(), '%Y-%m-%d')
    day_on_day_df = []
    day_long = []
    names = []
    date_list = get_workday_during_period(start_date, end_date, 0)  # get all first day of week during this period
    for m in range(1, num + 1):
        temp = ts / ts.shift(m, freq=freq) - 1
        temp1 = ts / ts.shift(m+2, freq=freq) - 1
        if m > 1:  # if want to compare to 2/3/4...period ,need to add the second day/third day ... to list
            date_list = date_list + get_workday_during_period(start_date, end_date, m-1)
        for day in date_list:
            temp[day] = temp1[day]
        names.append('compare_' + str(i) + freq)
        if len(day_on_day_df) == 0:
            day_on_day_df = temp
            day_long = temp
        else:
            day_on_day_df = pd.concat([day_on_day_df, temp], axis=1)
            day_long = pd.concat([day_long, temp])
    day_on_day_df.columns = names

    return day_on_day_df, day_long


FORMAT = '%Y-%m-%d %H:%M:%S'
figure_save_path = 'K://Algorithm_study_insuyan/result/study_yoy/test1221/figure1227/'
result_path = 'K://Algorithm_study_insuyan/result/study_yoy/test1221/result1227/'
read_path = 'K://Algorithm_study_insuyan/result/study_yoy/original_data/'

file_path_list = []
filename_list = []
listdir(read_path, file_path_list, filename_list)  # 遍历文件夹下所有的文件
print filename_list

for i in range(len(file_path_list)):
    read_data = pd.read_csv(file_path_list[i], delimiter='\t')
    filename = filename_list[i].split('.csv')[0]
    data_df = pd.DataFrame(read_data)
    data_df['clock'] = data_df['clock'].apply(timestamp_to_datetime)
    data_df['clock'] = data_df['clock'].apply(lambda x: datetime.datetime.strptime(x, FORMAT))

    data_series = data_df['value']
    data_series.index = data_df['clock']
    # print data_series['2017-11-05']

    data_series2 = data_series.resample('1T', how='mean', fill_method='ffill')  # re——sample data series

    data_max = data_series2.max()
    data_min = data_series2.min()

    data_series1 = data_series2.apply(lambda x: (x - data_min)/(data_max - data_min))
    print data_series1
    data_series1[data_series1 == 0] = 1e-3
    # data_series1['2017-11-10':'2017-11-13'].plot()
    # plt.show()

    start_date = '2017-10-30'
    end_date = '2017-11-03'

    five_compare_same, five_long = compare_same_period(data_series1, 5)
    mean_same = five_long[start_date: end_date].mean()
    std_same = five_long[start_date: end_date].std()
    same_data = five_compare_same[start_date: end_date]
    # same_data['up'] = mean_same + 3 * std_same
    # same_data['down'] = mean_same - 3 * std_same
    # same_data.to_csv(result_path + start_date + '_' + end_date + '_' +
    #             filename + '_MOM_with_3_std' + '.csv')
    same_up = mean_same + 6 * std_same
    same_down = mean_same - 6 * std_same

    print '*' * 100
    print same_up
    print same_down
    print same_data[(same_data > same_up).all(1)].ix[:, 1]

    five_compare_previous, five_previous_long = compare_previous_period(data_series1, 5)
    mean_previous = five_previous_long[start_date: end_date].mean()
    std_previous = five_previous_long[start_date: end_date].std()

    previous_data = five_compare_previous[start_date: end_date]
    previous_data['up'] = mean_previous + 6 * std_previous
    previous_data['down'] = mean_previous - 6 * std_previous

    previous_up = mean_previous + 6 * std_previous
    previous_down = mean_previous - 6 * std_previous

    print previous_up
    print previous_down
    print previous_data[previous_data.ix[:, 0] > previous_up]
    print '~' * 100
    # previous_data.to_csv(result_path + start_date + '_' + end_date + '_' +
    #             filename + '_DOD_with_3_std' + '.csv')

    fig, axes = plt.subplots(2, 1, sharex=True)
    # axes[0].plot(same_data)
    for x in range(5):
        axes[0].plot(same_data.ix[:, x])
        # anomaly_series1 = same_data[same_data.ix[:, x] > (mean_same + 3 * std_same)].ix[:, x]
        # anomaly_series2 = same_data[same_data.ix[:, x] < (mean_same - 3 * std_same)].ix[:, x]
        anomaly_series1 = same_data[same_data.ix[:, x] > (mean_same + 6 * std_same)].ix[:, x]
        anomaly_series2 = same_data[same_data.ix[:, x] < (mean_same - 6 * std_same)].ix[:, x]
        axes[0].plot(anomaly_series1, 'ko')
        axes[0].plot(anomaly_series2, 'ro')
        axes[0].set_title('Same period compare: min on min')
    # axes[0].legend(['MoM_1min', 'MoM_2min','MoM_3min','MoM_4min', 'MoM_5min', 'up','down'])

    # axes[1].plot(previous_data)
    # for x in range(5):
        axes[1].plot(previous_data.ix[:, x])
        axes[1].plot(x=0, y=previous_up)
        # anomaly_series1 = previous_data[(previous_data > previous_up).any(1)].ix[:, x]
        # anomaly_series2 = previous_data[(previous_data < previous_down).any(1)].ix[:, x]
        anomaly_series1 = previous_data[previous_data.ix[:, x] > (mean_previous + 6 * std_previous)].ix[:, x]
        anomaly_series2 = previous_data[previous_data.ix[:, x] < (mean_previous - 6 * std_previous)].ix[:, x]

        axes[1].plot(anomaly_series1, 'ko')
        axes[1].plot(anomaly_series2, 'ro')
    # axes[1].legend(['DoD_1day', 'DoD_2day', 'compare_3day', 'DoD_4day', 'DoD_5day', 'up', 'down'])

        axes[1].set_title('Previous period compare: day on day')
        axes[1].xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))  # 以天为单位分隔
        axes[1].xaxis.set_major_locator(DayLocator())
        fig.suptitle(start_date + ':' + end_date + ' ' + filename + '_with_3_std', )

    # plt.savefig(figure_save_path + start_date + '_' + end_date +
    #             '_' + filename + '_with_3_std' + '.png', dpi=300)
        plt.show()

