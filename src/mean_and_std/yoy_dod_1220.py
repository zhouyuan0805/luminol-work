#coding=utf-8
import pandas as pd
import numpy as np
import os
import time
import datetime
import matplotlib.pyplot as plt

def timestamp_to_datetime(x):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x))

def listdir(path, list_name, file_name):  #传入存储的list
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isdir(file_path):
                  listdir(file_path, list_name, file_name)
        else:
            list_name.append(file_path)
            file_name.append(file)

#compare with the same period of last day
def compare_same_period(ts, num):
    '''description:'''
    '''compare with the same period of last day'''
    min_on_min_df = []
    min_long = []
    names = []
    for i in range(1, num + 1):
        temp = ts / ts.shift(i) - 1
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
    '''description:'''
    '''compare with the previous period'''
    day_on_day_df = []
    day_long = []
    names = []
    for i in range(1, num + 1):
        temp = ts / ts.shift(i, freq=freq) - 1
        names.append('compare_'+str(i)+freq)
        if len(day_on_day_df) == 0:
            day_on_day_df = temp
            day_long = temp
        else:
            day_on_day_df = pd.concat([day_on_day_df, temp], axis=1)
            day_long = pd.concat([day_long, temp])
    day_on_day_df.columns = names
    return day_on_day_df, day_long

def detect_over_mean_std(ts_same, ts_previous, n, flag = 'up'):
    time_series = pd.concat([ts_same, ts_previous])
    mean = time_series.mean()
    std = time_series.std()
    print mean, std
    index = 1
    if flag == 'up':
        up = mean + n * std
        same_compare = ts_same - up
        previous_compare = ts_previous - up
    elif flag == 'down':
        down = mean - n * std
        same_compare = down - ts_same
        previous_compare = down - ts_previous
        index = -1
    else:
        print 'Error:check your parameter flag: up or down to choose!'
        return None
    same_compare = same_compare.apply(lambda x: index if x > 1e-8 else 0)
    previous_compare = previous_compare.apply(lambda x: index if x > 1e-8 else 0)
    return same_compare, previous_compare

def mark_up_down_label(same_compare_result,
                       previous_compare_result,
                       start='2017-11-23', end='2017-11-25', num=3):
    assign_same_series = same_compare_result[start:end]
    assign_previous_series = previous_compare_result[start:end]
    same_and_previous = pd.concat([assign_same_series, assign_previous_series])
    mean = same_and_previous.mean()
    std = same_and_previous.std()
    print mean, std
    up = mean + num * std
    down = mean - num * std
    def compare_three(x, y1, y2):
        if x > y1:
            return 1
        elif x < y2:
            return -1
        else:
            return 0
    same_df = pd.DataFrame(list(assign_same_series.values),columns=['value'])
    same_df.index = assign_same_series.index
    same_df['label'] = same_df['value'].apply(lambda x : compare_three(x, up, down))
    same_df['up'] = up
    same_df['down'] = down

    previous_df = pd.DataFrame(list(assign_previous_series.values), columns=['value'])
    previous_df.index = assign_previous_series.index
    previous_df['label'] = previous_df['value'].apply(lambda x: compare_three(x, up, down))
    previous_df['up'] = up
    previous_df['down'] = down
    return same_df, previous_df

def compare_previous_period_new(ts, num, start, end, freq='D'):
    '''description:'''
    '''compare with the previous period'''
    def get_workday_during_period(start, end, week_index=0):
        '''get one day of a week,like monday:0...'''
        test_date = pd.date_range(start, end)
        test_df = pd.DataFrame(test_date, columns=['date'])
        test_df['week_index'] = test_df['date'].apply(lambda x: x.weekday())
        c_list = list(test_df[test_df['week_index'] == week_index].ix[:, 'date'])
        for c in range(len(c_list)):
            c_list[c] = str(c_list[c]).split(' ')[0]
        return c_list

    day_on_day_df = []
    day_long = []
    names = []
    date_list = get_workday_during_period(start, end, 0)
    for i in range(1, num + 1):
        temp = ts / ts.shift(i, freq=freq) - 1
        temp1 = ts / ts.shift(i+2, freq=freq) - 1
        if i > 1:
            date_list = date_list + get_workday_during_period(start, end, i-1)
        for day in date_list:
            temp[day] = temp1[day]
        names.append('compare_'+str(i)+freq)
        if len(day_on_day_df) == 0:
            day_on_day_df = temp
            day_long = temp
        else:
            day_on_day_df = pd.concat([day_on_day_df, temp], axis=1)
            day_long = pd.concat([day_long, temp])
    day_on_day_df.columns = names
    return day_on_day_df, day_long


FORMAT = '%Y-%m-%d %H:%M:%S'
# read_path = 'K://Algorithm_study_insuyan/test_1207/data/'
figure_save_path = 'K://Algorithm_study_insuyan/result/study_yoy/test1221/figure2/'
result_path = 'K://Algorithm_study_insuyan/result/study_yoy/test1221/result_data/'

read_path = 'K://Algorithm_study_insuyan/result/study_yoy/original_data/'

file_path_list = []
filename_list = []
listdir(read_path, file_path_list, filename_list)
print filename_list

for i in range(len(filename_list)):
    read_data = pd.read_csv(file_path_list[i], delimiter='\t')
    filename = filename_list[i].split('.csv')[0]
    data_df = pd.DataFrame(read_data)
    data_df['clock'] = data_df['clock'].apply(timestamp_to_datetime).apply(lambda x: datetime.datetime.strptime(x,FORMAT) )

    data_series = data_df['value']
    data_series.index = data_df['clock']
    # print data_series['2017-11-05']
    #resampling data series one minute with average value --mean
    data_series1 = data_series.resample('1T', how='mean', fill_method='ffill')
    data_series1.plot()
    plt.show()

    # start_date = '2017-10-23'
    # end_date = '2017-10-27'
    #
    # five_compare_same, five_long = compare_same_period(data_series1, 5)
    # mean_same = five_long[start_date : end_date].mean()
    # std_same = five_long[start_date : end_date].std()
    # same_data = five_compare_same[start_date : end_date]
    # same_data['up'] = mean_same + 3 * std_same
    # same_data['down'] = mean_same - 3 * std_same
    # # same_data.to_csv(result_path + start_date + '_' + end_date + '_' +
    # #             filename + '_MOM_with_3_std' + '.csv')
    # previous_df, previous_long = compare_previous_period_new(data_series1, 5, '2017-10-22', '2017-11-25')
    # mean_previous = previous_long[start_date: end_date].mean()
    # std_previous = previous_long[start_date: end_date].std()
    # previous_data = previous_df[start_date: end_date]
    # previous_data['up'] = mean_previous + 3 * std_previous
    # previous_data['down'] = mean_previous - 3 * std_previous
    # # previous_data.to_csv(result_path + start_date + '_' + end_date + '_' +
    # #             filename + '_DOD_with_3_std' + '.csv')
    #
    # fig, axes = plt.subplots(2,1)
    # axes[0].plot(same_data)
    # for x in range(5):
    #     anomaly_series1 = same_data[same_data.ix[:,x] > (mean_same + 3 * std_same)].ix[:,x]
    #     anomaly_series2 = same_data[same_data.ix[:, x] < (mean_same - 3 * std_same)].ix[:, x]
    #     axes[0].plot(anomaly_series1,'ko')
    #     axes[0].plot(anomaly_series2, 'ro')
    # axes[0].set_title(start_date + ':' + end_date + ' ' + filename + '_with_3_std')
    # # axes[0].legend(['MoM_1min', 'MoM_2min','MoM_3min','MoM_4min', 'MoM_5min', 'up','down'])
    #
    # axes[1].plot(previous_data)
    # for x in range(5):
    #     anomaly_series1 = previous_data[previous_data.ix[:,x] > (mean_previous + 3 * std_previous)].ix[:,x]
    #     anomaly_series2 = previous_data[previous_data.ix[:, x] < (mean_previous - 3 * std_previous)].ix[:, x]
    #     axes[1].plot(anomaly_series1,'ko')
    #     axes[1].plot(anomaly_series2, 'ro')
    # axes[1].legend(['DoD_1day', 'DoD_2day', 'compare_3day', 'DoD_4day', 'DoD_5day', 'up', 'down'])


    # plt.show()





























    # workdays_list = ['2017-10-23', '2017-10-24', '2017-10-25', '2017-10-26', '2017-10-27',
    #                  '2017-10-30', '2017-10-31', '2017-11-01', '2017-11-02', '2017-11-03',
    #                  '2017-11-06', '2017-11-07', '2017-11-08', '2017-11-09', '2017-11-10',
    #                  '2017-11-13', '2017-11-14', '2017-11-15', '2017-11-16', '2017-11-17',
    #                  '2017-11-20', '2017-11-21', '2017-11-22', '2017-11-23', '2017-11-24']





    # print previous_df['2017-11-2'].head()
    # # print data_series1['2017-11-2'].head()
    # print data_series1['2017-11-2'].head()
    # print data_series1['2017-11-1'].head()
    # print data_series1['2017-10-31'].head()
    # print data_series1['2017-10-30'].head()
    # print data_series1['2017-10-27'].head()
    # print data_series1['2017-10-26'].head()
    # # print data_series1['2017-10-25'].head()
    # # print data_series1['2017-10-24'].head()
    # # print data_series1['2017-10-23'].head()
    # print '*'*100
    # # print previous_long.dropna()

    # print c_list
    # print data_series1[c_list[3]]
    # print data_series1[workdays_list[j]]/data_series1[workdays_list[j+1]]-1








