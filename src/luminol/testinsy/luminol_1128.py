# -*- coding: utf-8 -*-
import os
import pandas as pd
import datetime
import time
import numpy as np
import matplotlib.pyplot as plt
import csv
from luminol.anomaly_detector import AnomalyDetector
import luminol.utils

def timestamp_to_datetime(x):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(x))

def listdir(path, list_name, file_name):  #传入存储的list
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isdir(file_path):
                  listdir(file_path, list_name)
        else:
            list_name.append(file_path)
            file_name.append(file)

in_path = 'K://Algorithm_study_insuyan/data/luminol/mpp/'
ewm_path = 'K://Algorithm_study_insuyan/data/luminol/mpp_ewm/'
ewm_score_path = 'K://Algorithm_study_insuyan/data/luminol/mpp_ewm_score/'
figure_ewm = 'K://Algorithm_study_insuyan/data/luminol/figure_ewm/'

FORMAT1 = '%Y-%m-%d %H:%M:%S'

filepath_list = []
filename_list = []
filename = []

listdir(in_path, filepath_list, filename_list)
for i in filename_list:
    filename.append(os.path.splitext(i)[0])

# for i in np.arange(len(filepath_list)):
for i in range(151, len(filepath_list)):
    test_data = pd.read_csv(filepath_list[i], delimiter='\t')
    test_df = pd.DataFrame(test_data)

    test_series = test_df['value']
    test_series.index = test_df['clock'].apply(timestamp_to_datetime)

    ewm_data = luminol.utils.compute_ema(0.2, test_series)

    ewm_series = pd.Series(ewm_data)
    ewm_series.index = test_series.index

    ewm_data_df = pd.DataFrame(list(zip(ewm_series.index, ewm_series.values)))
    ewm_data_df.columns = ['kpi_time', 'kpi_value']

    ewm_data_df.to_csv(ewm_path+filename[i]+'_ewm.csv', index=False, header=False)
    #detect ewm series data
    my_detector = AnomalyDetector(ewm_path+filename[i]+'_ewm.csv')
    score = my_detector.get_all_scores()
    with open(ewm_score_path + filename[i] + '_score_50_200.csv', "w") as csvfile:
        writer = csv.writer(csvfile)
        for timestamp, value in score.iteritems():
            t_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))
            writer.writerow([t_str, value])
    csvfile.close()

    ewm_data = pd.read_csv(ewm_path + filename[i] + '_ewm.csv', delimiter=',',
                                   names=['kpi_time', 'kpi_value'], index_col=0, parse_dates=True)

    result_ewm_score = pd.read_csv(ewm_score_path + filename[i] + '_score_50_200.csv', delimiter=',',
                               names=['kpi_time', 'kpi_value'], index_col=0,parse_dates=True)

    fig1 = plt.figure()
    ax1 = fig1.add_subplot(211)
    ewm_data.plot(ax=ax1, style='k.-')
    ax1.legend(['rolling mean series'])
    ax1.set_title(filename[i] + '_ewm_0.2_50_200')
    ax2 = fig1.add_subplot(212)
    result_ewm_score.plot(ax=ax2, style='b-')
    ax2.legend(['score series'])
    plt.savefig(figure_ewm + filename[i]+'_ewm_50_200.png', dpi=300)
    plt.close(fig1)
    print '-----------------------' + str(i) + '-------------------------'
    # plt.show()


    # print filename[i]
    # print '-'*40
    # print ewa_series
    # print '*'*40
    # rolling_ewma_data = pd.ewma(test_series, alpha=0.2)
    # rolling_ewma_data = rolling_ewma_data.apply(lambda x: str(x))
    # # # fig = plt.figure()
    # #
    # #
    # print rolling_ewma_data
    # print '~'*40
    # print test_series
    #
    #
    # rolling_mean_data = pd.rolling_mean(test_series,3)
    # rolling_median_data = pd.rolling_median(test_series,3)
    #
    # rolling_mean_data[rolling_mean_data.isnull()] = test_series[rolling_mean_data.isnull()]
    # rolling_median_data[rolling_median_data.isnull()] = test_series[rolling_median_data.isnull()]
    #
    #
    #
    # mean_data_df = pd.DataFrame(list(zip(rolling_mean_data.index, rolling_mean_data.values)))
    # mean_data_df.columns = ['kpi_time', 'kpi_value']
    # # mean_data_df.to_csv(mean_path+filename[i]+'_rolling_mean.csv', index=False, header=False)
    #
    # median_data_df = pd.DataFrame(list(zip(rolling_median_data.index, rolling_median_data.values)))
    # median_data_df.columns =['kpi_time', 'kpi_value']
    # # median_data_df.to_csv(median_path+filename[i]+'_rolling_median.csv', index=False, header=False)
    #
    # print '-----------------------'+str(i)+'-------------------------'
    # # print mean_data_df.head()
    # # print median_data_df.head()
    #
    # #detect rolling mean series data
    # my_detector = AnomalyDetector(mean_path+filename[i]+'_rolling_mean.csv')
    # score = my_detector.get_all_scores()
    # with open(mean_score_path + filename[i] + '_score_50_200.csv', "w") as csvfile:
    #     writer = csv.writer(csvfile)
    #     for timestamp, value in score.iteritems():
    #         t_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))
    #         writer.writerow([t_str, value])
    # csvfile.close()
    # result_mean_score = pd.read_csv(mean_score_path + filename[i] + '_score_50_200.csv', delimiter=',',
    #                            names=['kpi_time', 'kpi_value'], index_col=0,parse_dates=True)
    #
    #
    # #detect rolling median series data
    # my_detector = AnomalyDetector(median_path+filename[i]+'_rolling_median.csv')
    # score = my_detector.get_all_scores()
    # with open(median_score_path + filename[i] + '_median_score_50_200.csv', "w") as csvfile:
    #     writer = csv.writer(csvfile)
    #     for timestamp, value in score.iteritems():
    #         t_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))
    #         writer.writerow([t_str, value])
    # csvfile.close()
    # result_median_score = pd.read_csv(median_score_path + filename[i] + '_median_score_50_200.csv', delimiter=',',
    #                            names=['kpi_time', 'kpi_value'], index_col=0,parse_dates=True)
    #
    #
    # # result_series = result_score['kpi_value']
    # # result_series.index = result_score['kpi_time'].apply(lambda x: datetime.datetime.strftime(x, FORMAT1))
    #
    # mean_series = mean_data_df['kpi_value']
    # mean_series.index = mean_data_df['kpi_time'].apply(lambda x: datetime.datetime.strptime(x,FORMAT1))
    #
    # median_series = median_data_df['kpi_value']
    # median_series.index = median_data_df['kpi_time'].apply(lambda x: datetime.datetime.strptime(x,FORMAT1))
    #
    #
    # fig1 = plt.figure()
    # ax1 = fig1.add_subplot(211)
    # mean_series.plot(ax=ax1, style='k.-')
    # ax1.legend(['rolling mean series'])
    # ax1.set_title(filename[i] + 'rolling_mean_50_200')
    # ax2 = fig1.add_subplot(212)
    # result_mean_score.plot(ax=ax2, style='b-')
    # ax2.legend(['score series'])
    # plt.savefig(figure_mean + filename[i]+'rolling_mean_50_200.png', dpi=300)
    #
    # fig2 = plt.figure()
    # ax3 = fig2.add_subplot(211)
    # median_series.plot(ax=ax3, style='k.-')
    # ax3.legend(['rolling median series'])
    # ax3.set_title(filename[i] + 'rolling_median_50_200')
    # ax4 = fig2.add_subplot(212)
    # result_median_score.plot(ax=ax4, style='b-')
    # ax4.legend(['score series'])
    # plt.savefig(figure_median + filename[i] + 'rolling_median_50_200.png', dpi=300)






# print test_series.head()
# print rolling_mean_data.head()
# print rolling_median_data.head()
