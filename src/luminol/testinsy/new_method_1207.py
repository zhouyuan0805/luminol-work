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


FORMAT = '%Y-%m-%d %H:%M:%S'
read_path = 'K://Algorithm_study_insuyan/test_1207/data/'
file_name = ''


file_path_list = []
filename_list = []
listdir(read_path, file_path_list, filename_list)

read_data = pd.read_csv(file_path_list[0], delimiter='\t')
data_df = pd.DataFrame(read_data)
data_df['clock'] = data_df['clock'].apply(timestamp_to_datetime).apply(lambda x: datetime.datetime.strptime(x,FORMAT) )

data_series = data_df['value']
data_series.index = data_df['clock']

#resampling data series one minute with average value --mean
data_series1 = data_series.resample('1T', how='mean', fill_method='ffill')
#compute YoY -- 天同比
YoY = compare_same_period(data_series1, 6)
#compute DoD -- 分环比
DoD = compare_previous_period(data_series1, 4)
summary_df = pd.concat([YoY, DoD],axis=1)

mean_series = summary_df.mean(1)
std_series = summary_df.std(1)

up_series = mean_series + 2 * std_series
down_series = mean_series - 2 * std_series

test_YoY = YoY.copy()
for i in test_YoY.columns:
    test_YoY[i] = test_YoY[i] - up_series
    test_YoY[i] = test_YoY[i].apply(lambda x: 1 if x > 1e-6 else 0)
    # print test_YoY[test_YoY[i]==1]
tt = test_YoY.sum(1)
print tt[tt>0]
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
data_series1.plot()
# up_series.plot(linestyle='-')
# down_series.plot()
# plt.legend(['data_series', 'up_series'])
plt.show()




# print compare_same_period(data_series1, 6)
#
# print '*'*100
#
# print compare_previous_period(data_series1, 4)







# print data_series1
# print len(data_series1)
#
# print data_series1.shift(freq='1T')
# print len(data_series1)
# print data_series1
# print data_series1.shift(1,freq='D')
# print data_series1.shift(2,freq='D')
# d = data_series1/data_series1.shift(2,freq='D') -1
# print d.dropna()








# huanbi_series = [date_series2[s.index-1] for s in data_series2]




#
# print data_series1[data_series1.isnull()]
# print data_series2[data_series2.isnull()]

# print data_series['2017-10-22 05:20:00':'2017-10-22 05:30:00']
# print data_series1['2017-10-22 05:20:00':'2017-10-22 05:30:00']
# print data_series2['2017-10-22 05:20:00':'2017-10-22 05:30:00']
# print data_series1['2017-10-22 05:22:00']
# print data_series1['2017-10-22 05:21:00']
# print len(time_test)


