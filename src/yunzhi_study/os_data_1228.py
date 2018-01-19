#coding=utf-8

import pandas as pd
import time
import datetime
import matplotlib.pyplot as plt
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
    temp =[]
    for timestamp, value in score.iteritems():
        temp.append([timestamp_to_datetime(timestamp/1000), value])
    temp_df = pd.DataFrame(temp, columns=['kpi_time', 'kpi_value'])
    return temp_df


FORMAT1 = '%Y-%m-%d %H:%M:%S'
read_path = 'K://Algorithm_study_insuyan/result/OperationSupport/data/'
save_figure = 'K://Algorithm_study_insuyan/result/OperationSupport/figure/'


data_read = pd.read_excel(read_path + '2.4 tomcat mem.xlsx')

# print data_read.groupby('itemid').count()
# data_read.to_csv(read_path + '2.4_tomcat_mem.csv', index=False)
print data_read
test_series = data_read['value']
data_read['clock'] = data_read['clock'].apply(timestamp_to_datetime)
test_series.index = data_read['clock'].apply(lambda x: datetime.datetime.strptime(x, FORMAT1))

print test_series

test_series2 = test_series.resample('1T', how='mean').bfill()


compare_same = test_series2 / test_series2.shift(1, freq='1T')
print compare_same.dropna()
compare_same.to_csv(read_path + 'resample_tomcat_data.csv', header=False)

my_detector = AnomalyDetector(read_path + 'resample_tomcat_data.csv')
score = my_detector.get_all_scores()
score_series = df_to_series(score_to_df(score))
print score_series

# print score
# print compare_same
# print test_series
fig, axes = plt.subplots(3, 1, sharex=True)

axes[0].plot(test_series2)
axes[1].plot(compare_same)
axes[2].plot(score_series)

axes[0].legend(['resamlpe series'], loc='best')
axes[0].set_title('resample 1 min from original series')
axes[1].legend(['min-on-min compare series'], loc='best')
axes[1].set_title('compare current data to previous min data result series')
axes[2].legend(['score series'])
axes[2].set_title('luminol algorithm detect min-on-min series result: score')

# compare_same.plot(ax=ax1)

# axes[2].xaxis.set_minor_locator(dates.WeekdayLocator(byweekday=(1), interval=1))
# axes[2].xaxis.set_minor_formatter(dates.DateFormatter('%d\n%a'))
# axes[2].xaxis.set_major_locator(dates.MonthLocator())
# axes[2].xaxis.set_major_formatter(dates.DateFormatter('\n\n\n%b\n%Y'))
# plt.tight_layout()


axes[2].xaxis.set_major_formatter(DateFormatter('\n%m-%d\n%Y'))  # 以天为单位分隔
axes[2].xaxis.set_major_locator(DayLocator())


# ax.xaxis.set_minor_locator(dates.WeekdayLocator(byweekday=(1),
#                                                 interval=1))
# ax.xaxis.set_minor_formatter(dates.DateFormatter('%d\n%a'))
# ax.xaxis.grid(True, which="minor")
# ax.yaxis.grid()
# ax.xaxis.set_major_locator(dates.MonthLocator())
# ax.xaxis.set_major_formatter(dates.DateFormatter('\n\n\n%b\n%Y'))
# plt.tight_layout()

plt.suptitle('Anomaly detect result figure: 1min-on-1min')
plt.savefig(save_figure + 'resample_MoM_LuminolScore.png', dpi=300)

plt.show()
