#coding=utf-8

import pandas as pd
import time
import datetime
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.dates import DayLocator, HourLocator, DateFormatter
from luminol.anomaly_detector import AnomalyDetector
import matplotlib.dates as dates


def timestamp_to_datetime(x):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x))


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
    temp = []
    for timestamp, value in score_data.iteritems():
        temp.append([timestamp_to_datetime(timestamp/1000), value])
    temp_df = pd.DataFrame(temp, columns=['kpi_time', 'kpi_value'])
    return temp_df


FORMAT1 = '%Y-%m-%d %H:%M:%S'
read_path = 'K://Algorithm_study_insuyan/result/OperationSupport/data/'
save_figure = 'K://Algorithm_study_insuyan/result/OperationSupport/figure/'


data_read = pd.read_excel(read_path + '2.4 tomcat mem.xlsx')

# print data_read.groupby('itemid').count()
# data_read.to_csv(read_path + '2.4_tomcat_mem.csv', index=False)
# print data_read
test_series = data_read['value']
data_read['clock'] = data_read['clock'].apply(timestamp_to_datetime)
test_series.index = data_read['clock'].apply(lambda x: datetime.datetime.strptime(x, FORMAT1))

# print test_series

test_series2 = test_series.resample('1T', how='mean').bfill()
# test_series2.to_csv('sample.csv', header=False)

three_df = pd.DataFrame(list(zip(test_series2['2017-12-25'].values,
                                 test_series2['2017-12-26'].values,
                                 test_series2['2017-12-27'].values)), columns=['12-25', '12-26', '12-27'])

print '------before : three df concat ------'
print three_df.head()

for i in range(len(three_df.columns)):
    three_df.ix[:, i] = three_df.ix[:, i] - three_df.ix[:, i].min()

three_df['mean'] = three_df.mean(1)
three_df['median'] = three_df.median(1)

three_df['five'] = three_df['12-25'] / three_df['mean']
three_df['six'] = three_df['12-26'] / three_df['mean']
three_df['seven'] = three_df['12-27'] / three_df['mean']

print '------end : three df concat ------'
print three_df.head()
three_df[['five', 'six', 'seven']].plot()


# compare_same = test_series2 / test_series2.shift(1, freq='1T')
# compare_same.to_csv('MoM_1min_data.csv')
#
# my_detector1 = AnomalyDetector('sample.csv')
# score1 = my_detector1.get_all_scores()
# score_df1 = score_to_df(score1)
# score_series1 = df_to_series(score_df1)
#
# my_detector2 = AnomalyDetector('MoM_1min_data.csv')
# score2 = my_detector2.get_all_scores()
# score_df2 = score_to_df(score2)
# score_series2 = df_to_series(score_df2)
#
# print score_series1


# ---------result figure 2----------
# fig, (fig1, fig2, fig3) = plt.subplots(3, 1)
# fig1.plot(test_series2['2017-12-25'])
# fig2.plot(compare_same['2017-12-25'])
# fig3.plot(score_series2['2017-12-25'])
# plt.suptitle('2017-12-25')
# fig1.set_title('resample series')
# fig2.set_title('MoM series')
# fig3.set_title('luminol detect MoM score series')

# --------result 1 --------
# fig, (fg1, fg2, fg3) = plt.subplots(3,1)
# fg1.plot(score_series['2017-12-25'])
# fg2.plot(score_series['2017-12-26'])
# fg3.plot(score_series['2017-12-27'])
# fg1.set_title('2017-12-25')
# fg2.set_title('2017-12-26')
# fg3.set_title('2017-12-27')
# test_series2['2017-12-25'].plot()
# test_series2['2017-12-26'].plot()
# test_series2['2017-12-27'].plot()


# test_series3 = test_series2.apply(lambda x: np.log10(x))
# print test_series3
# compare_same = test_series2 / test_series2.shift(1, freq='1T')
# cp_test = test_series3 / test_series3.shift(1, freq='1min') - 1
#
# compare_same.plot()
# fig, axes = plt.subplots(2, 2, sharex=True)
# axes[0, 0].plot(test_series2)
# axes[0, 1].plot(test_series3)
# axes[1, 0].plot(compare_same)
# axes[1, 1].plot(cp_test)
# axes[0, 0].xaxis.set_major_formatter(DateFormatter('\n%m\n%d\n%Y'))  # 以天为单位分隔
# axes[0, 0].xaxis.set_major_locator(DayLocator())
# axes[1, 1].xaxis.set_major_formatter(DateFormatter('\n%m\n%d\n%Y'))  # 以天为单位分隔
# axes[1, 1].xaxis.set_major_locator(DayLocator())
# test_series3.plot()
plt.show()


# print compare_same.dropna()


# compare_same.to_csv(read_path + 'resample_tomcat_data.csv', header=False)
#
# my_detector = AnomalyDetector(read_path + 'resample_tomcat_data.csv')
# score = my_detector.get_all_scores()
# score_series = df_to_series(score_to_df(score))
# print score_series

# print score
# print compare_same
# print test_series
# fig, axes = plt.subplots(3, 1, sharex=True)
#
# axes[0].plot(test_series2)
# axes[1].plot(compare_same)
# axes[2].plot(score_series)
#
# axes[0].legend(['resamlpe series'], loc='best')
# axes[0].set_title('resample 1 min from original series')
# axes[1].legend(['min-on-min compare series'], loc='best')
# axes[1].set_title('compare current data to previous min data result series')
# axes[2].legend(['score series'])
# axes[2].set_title('luminol algorithm detect min-on-min series result: score')
#
#
# axes[2].xaxis.set_major_formatter(DateFormatter('\n%m-%d\n%Y'))  # 以天为单位分隔
# axes[2].xaxis.set_major_locator(DayLocator())
#

# ax.xaxis.set_minor_locator(dates.WeekdayLocator(byweekday=(1),
#                                                 interval=1))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d\n%a'))
# ax.xaxis.grid(True, which="minor")
# ax.yaxis.grid()
# ax.xaxis.set_major_locator(dates.MonthLocator())
# ax.xaxis.set_major_formatter(dates.DateFormatter('\n\n\n%b\n%Y'))
# plt.tight_layout()

# plt.suptitle('Anomaly detect result figure: 1min-on-1min')
# plt.savefig(save_figure + 'resample_MoM_LuminolScore.png', dpi=300)
#
# plt.show()
