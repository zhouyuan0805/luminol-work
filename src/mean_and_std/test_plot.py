import matplotlib.pyplot as plt
import pandas as pd
import datetime

FORMAT = '%Y-%m-%d %H:%M:%S'
# data = pd.read_csv('K://Algorithm_study_insuyan/result/study_yoy/test1221/result_data/2017-11-06_2017-11-10_10.17.1.3_Available memory_DOD_with_3_std.csv')

score_data = pd.read_csv('K://Algorithm_study_insuyan/data/10.17.1.3_Available memory_score_50_200.csv',
                    names=['clock', 'value'])

original_data = pd.read_csv('K://Algorithm_study_insuyan/data/10.17.1.3_Available memory_rolling_mean.csv',
                            names=['clock', 'value'])

original_series = original_data['value']
original_series.index = pd.to_datetime(original_data['clock'])

score_series = score_data['value']
score_series.index = pd.to_datetime(score_data['clock'])

original_1min_series = original_series.resample('60S',how='bfill')
score_1min_series = score_series.resample('60S',how='bfill')

original_15s_series = original_1min_series.resample('15S',how='bfill')
score_15s_series = score_1min_series.resample('15S',how='bfill')
print original_15s_series
print score_15s_series
# original_15s_series.to_csv('K://Algorithm_study_insuyan/data/10.17.1.3_Available memory_15s_original_data.csv',  header=False)
# score_15s_series.to_csv('K://Algorithm_study_insuyan/data/10.17.1.3_Available memory_15s_score_data.csv',  header=False)


fig, axes = plt.subplots(2, 1)
# axes[0].plot(original_series)
# axes[1].plot(score_series)

# axes[0].plot(original_1min_series)
# axes[1].plot(score_1min_series)

axes[0].plot(original_15s_series['2017-11-01'])
axes[1].plot(score_15s_series['2017-11-01'])

plt.show()

# score_series = data2['value']
# score_series.index = pd.to_datetime(data2['clock'])
#
# re
# sample_score_1min = score_series.resample('60S',how='bfill')
# resample_score_15s = resample_score_1min.resample('15s', how = 'ffill')
# resample_score_15s.plot()
# plt.show()
# print data2
# data2.to_csv('K://Algorithm_study_insuyan/data/10.17.1.3_Available memory_score.csv', index=False, header=False)

# test_series = data['compare_1D']
# # test_series.index = data['clock'].apply(lambda x: datetime.datetime.strptime(x, FORMAT))
# test_series.index = pd.to_datetime(data['clock'])
# print type(data)
# test_series.plot()
# plt.show()