#coding=utf-8

import pandas as pd
import time
import datetime
import matplotlib.pyplot as plt
import xlrd
import numpy as np
from matplotlib.dates import DayLocator, HourLocator, DateFormatter
from luminol.anomaly_detector import AnomalyDetector
import matplotlib.dates as dates


def timestamp_to_datetime(x):
    '''
    :param x: timestamp data
    :return: YYYY-mm-dd HH:MM:SS
    '''
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

tomcat_list = ['2.4', '2.5']

for i in range(len(tomcat_list)):
    # 读取Excel文件数据
    data_read = pd.read_excel(tomcat_list[i] + 'mem.xlsx')

    # 从读取的数据中，制作 time：value 的时间序列数据Series
    test_series = data_read['value']
    data_read['clock'] = data_read['clock'].apply(timestamp_to_datetime)
    test_series.index = data_read['clock'].apply(lambda x: datetime.datetime.strptime(x, FORMAT1))

    # print test_series

    # 数据规整，按照一分钟一条规整数据，用均值和向前填充的方式
    test_series2 = test_series.resample('1T', how='mean').bfill()
    test_series2.to_csv(tomcat_list[i] + 'mem_long_sample.csv', header=False)

    # 计算同比，此时此刻的数据比上前一天的数据
    dod_ts = test_series2 / test_series2.shift(1, freq='1D') - 1
    # 计算环比，此时此刻的数据比上前一分钟的数据
    mom_ts = test_series2 / test_series2.shift(1, freq='1T') - 1

    dod_ts.to_csv(tomcat_list[i] + 'dod_ts.csv')
    mom_ts.to_csv(tomcat_list[i] + 'mom_ts.csv')


    # --------- mom analysis view show --------------
    # # 1：使用luminol算法检测数据，取出score
    # my_detector = AnomalyDetector(tomcat_list[i] + 'mom_ts.csv')
    # score = my_detector.get_all_scores()
    # score_df = score_to_df(score)
    # score_ts = df_to_series(score_df)
    #
    # # 2： 可视化展示  >1 原始数据采样后的数据 >2 环比数据 >3 luminol 算法得分数据
    # fig, (fig1, fig2, fig3) = plt.subplots(3, 1, sharex=True)
    # fig1.plot(test_series2)
    # fig1.set_title('original: resample 1 min series')
    # fig1.grid(True)
    # fig2.plot(mom_ts)
    # fig2.set_title('Min On Min series')
    # fig2.grid(True)
    # fig3.plot(score_ts)
    # fig3.set_title('luminol detect mom series return score series')
    # fig3.grid(True)
    # fig3.xaxis.set_major_formatter(DateFormatter('\n%m\n%d\n%Y'))  # 以天为单位分隔
    # fig3.xaxis.set_major_locator(DayLocator())
    # plt.suptitle('tomcat ' + tomcat_list[i] + ' memory mom analysis')



    # ---------- dod analysis view show ---------------
    # 1：使用luminol算法检测数据，取出score
    my_detector = AnomalyDetector(tomcat_list[i] + 'dod_ts.csv')
    score = my_detector.get_all_scores()
    score_df = score_to_df(score)
    score_ts = df_to_series(score_df)

    # 2 画图展示
    fig, (fig1, fig2, fig3) = plt.subplots(3, 1, sharex=True)
    fig1.plot(test_series2)
    fig1.set_title('original: resample 1 min series')
    fig1.grid(True)
    fig2.plot(dod_ts)
    fig2.set_title('dod series')
    fig2.grid(True)
    fig3.plot(score_ts)
    fig3.set_title('luminol detect dod series return score series')
    fig3.grid(True)
    fig3.xaxis.set_major_formatter(DateFormatter('\n%m\n%d\n%Y'))  # 以天为单位分隔
    fig3.xaxis.set_major_locator(DayLocator())
    plt.suptitle('tomcat ' + tomcat_list[i] + ' memory dod analysis')

    plt.show()
