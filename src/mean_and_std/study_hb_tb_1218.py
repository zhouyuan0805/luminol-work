#coding=utf-8
import pandas as pd
import numpy as np
import os
import time
import datetime
import matplotlib.pyplot as plt

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

#compare with the same period of last day
def compare_same_period(ts, num):
    '''description:'''
    '''compare with the same period of last day'''
    min_on_min_df = []
    names = []
    for i in range(1, num + 1):
        temp = ts / ts.shift(i) - 1
        names.append('compare_' + str(i) + 'min')
        if len(min_on_min_df) == 0:
            min_on_min_df = temp
        else:
            min_on_min_df = pd.concat([min_on_min_df, temp], axis=1)
    min_on_min_df.columns = names
    return min_on_min_df

def compare_previous_period(ts, num, freq='D'):
    '''description:'''
    '''compare with the previous period'''
    day_on_day_df = []
    names = []
    for i in range(1, num + 1):
        temp = ts / ts.shift(i, freq=freq) - 1
        names.append('compare_'+str(i)+freq)
        if len(day_on_day_df) == 0:
            day_on_day_df = temp
        else:
            day_on_day_df = pd.concat([day_on_day_df, temp], axis=1)
    day_on_day_df.columns = names
    return day_on_day_df

def detect_over_mean_std(ts_same, ts_previous, n, flag = 'up'):
    time_series = pd.concat([ts_same,ts_previous])
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

FORMAT = '%Y-%m-%d %H:%M:%S'
# read_path = 'K://Algorithm_study_insuyan/test_1207/data/'
figure_save_path = 'K://Algorithm_study_insuyan/result/study_yoy/figure/'
result_path = 'K://Algorithm_study_insuyan/result/study_yoy/result_data/'


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
    print data_series['2017-11-05']
    #resampling data series one minute with average value --mean
    data_series1 = data_series.resample('1T', how='mean', fill_method='ffill')
    print data_series1['2017-11-05']
    data_series.plot()
    plt.show()
    # compare_same = data_series1 / data_series1.shift(1) - 1
    # compare_previous = data_series1 / data_series1.shift(1, freq = '7D') - 1
    # same_ts = compare_same.dropna()
    # previous_ts = compare_previous.dropna()
    #
    # def plot_and_save_figure(same_ts_in, previous_ts_in,
    #                          start='2017-11-23',
    #                          end='2017-11-25', num=2):
    #
    #     window_same, window_previous = mark_up_down_label(same_ts_in, previous_ts_in,
    #                                                       start='2017-11-23',
    #                                                       end='2017-11-25', num=num)
    #     window_same.to_csv(result_path + start + '_' + end + '_' + filename +
    #                        '_with_' + str(num) + 'std' + '_same_comp.csv', index= False,)
    #     window_previous.to_csv(result_path + start + '_' + end + '_' + filename +
    #                        '_with_' + str(num) + 'std' + '_previous_comp.csv', index=False)
    #
    #     fig, axes = plt.subplots(2, 1)
    #     axes[0].plot(window_same[['value', 'up', 'down']])
    #     axes[0].legend(['MoM', 'up', 'down'])
    #     axes[0].set_title(start + '_' + end + '_' + filename + '_with_' + str(num) + 'std')
    #     axes[0].plot(window_same['value'][window_same['label']!=0],'ko')
    #     axes[1].plot(window_previous[['value', 'up', 'down']])
    #     axes[1].plot(window_previous['value'][window_previous['label'] >= 0], 'ko')
    #     axes[1].legend(['WoW', 'up', 'down'])
    #     plt.savefig(figure_save_path + start + '_' + end + '_' + filename + '_with_' + str(num) + 'std' + '.png', dpi=300)
    #
    # plot_and_save_figure(same_ts, previous_ts,start='2017-11-24',
    #                      end='2017-11-25', num=3)
    # # window_same.plot()
    # # window_previous.plot()
    # plt.show()


# windows1_t2 = t2['2017-11-13':'2017-11-17']
# windows1_t1 = t1['2017-11-13':'2017-11-17']
#
# windows_same_1113 = same_ts['2017-11-13']
# windows_previous_1113 = previous_ts['2017-11-13']
# #
# # windows2_t2 = t2['2017-11-20':'2017-11-24']
# # windows2_t1 = t1['2017-11-20':'2017-11-24']
#
# window1 = pd.concat([windows_same_1113, windows_same_1113])
# # window2 = pd.concat([windows2_t1, windows2_t2])
#
#
# window1_flag_up = detect_over_mean_std(window1, 3, flag='up')
# window1_flag_down = detect_over_mean_std(window1, 3, flag='down')
# window1_flag_up.plot()
# # windows1_t2.plot()
#
#


# print window1_flag_up
# print window1_flag_down
#
# window1_other = detect_over_mean_std(window1, 2, flag='other')
# print window1_other


# mean = window2.mean()
# std = window2.std()
#
# up_threshold = mean + 3 * std
# down_threshold = mean - 3 * std
#
# window2_t1_flag = windows2_t1 - up_threshold
# window2_t1_flag = window2_t1_flag.apply(lambda x: 1 if x > 1e-8 else 0)





# window2_t1_flag.plot(color='red')
# # windows1_t1.plot()
# windows2_t1.plot()
# plt.show()


#compute YoY -- 天同比
# YoY = compare_same_period(data_series1, 6)
#compute DoD -- 分环比
# DoD = compare_previous_period(data_series1, 4)
# summary_df = pd.concat([YoY, DoD],axis=1)

# mean_series = summary_df.mean(1)
# std_series = summary_df.std(1)
#
# up_series = mean_series + 2 * std_series
# down_series = mean_series - 2 * std_series
#
# test_YoY = YoY.copy()
# for i in test_YoY.columns:
#     test_YoY[i] = test_YoY[i] - up_series
#     test_YoY[i] = test_YoY[i].apply(lambda x: 1 if x > 1e-6 else 0)
#     # print test_YoY[test_YoY[i]==1]
# tt = test_YoY.sum(1)
# print tt[tt>0]
# print test_YoY



# for i in range(len(YoY)):
#     print[1 if YoY.ix[i, 1] > up_series[i] else 0]
# day_des = YoY - up_series
# second_des = DoD - up_series
#
# print day_des
# print '*'*100
# print second_des

# up_series['2017-10'].plot()
# mean_series['2017-10'].plot()
# down_series['2017-10'].plot()

# mean_series.plot()
# data_series1.plot()
# up_series.plot(linestyle='-')
# down_series.plot()
# plt.legend(['data_series', 'up_series'])
# plt.show()

