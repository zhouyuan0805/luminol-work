#coding=utf-8

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import csv
import datetime
import time
from luminol.anomaly_detector import AnomalyDetector


read_path = 'K://Algorithm_study_insuyan/data/DATA_TEST_LML/data/'
write_path = 'K://Algorithm_study_insuyan/data/DATA_TEST_LML/result/'

filename = ['10.17.1.1_Available_memory', '10.17.1.1_Context switches per second',
            '10.17.1.1_CPU user time', '10.17.1.1_Incoming network traffic on bond6',
            '10.17.1.1_Outgoing network traffic on bond6', '10.17.1.1_Processor load (1 min average per core)']

FORMAT1 = '%Y-%m-%d %H:%M:%S'
def timestamp_to_datetime(x):
    return time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(x))

for i in np.arange(len(filename)) :
    read_data = pd.read_csv(read_path + filename[i]+'.csv', delimiter='\t')

    detect_df = pd.DataFrame(read_data)
    detect_df['clock'] = detect_df['clock'].apply(timestamp_to_datetime)
    detect_df.to_csv(write_path+'/detect_data/'+filename[i]+'_detect.csv', index=False, header=False)

    my_detector = AnomalyDetector(write_path + '/detect_data/' + filename[i] + '_detect.csv')

    score = my_detector.get_all_scores()

    with open(write_path + '/score_data/' + filename[i] + '_score_1440_100.csv', "w") as csvfile:
        writer = csv.writer(csvfile)
        for timestamp, value in score.iteritems():
            t_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp / 1000))
            writer.writerow([t_str, value])
    csvfile.close()



    result_score = pd.read_csv(write_path+'/score_data/'+filename[i]+'_score_1440_100.csv', delimiter=',',
                               names=['kpi_time', 'kpi_value'], index_col=0,parse_dates=True)
    # result_series = result_score['kpi_value']
    # result_series.index = result_score['kpi_time'].apply(lambda x: datetime.datetime.strftime(x, FORMAT1))

    detect_series = detect_df['value']
    detect_series.index = detect_df['clock'].apply(lambda x: datetime.datetime.strptime(x,FORMAT1))

    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    detect_series.plot(ax=ax1, style='k.-')
    ax1.legend(['original series'])
    ax1.set_title(filename[i] + 'detect_compare_min100_max1440')

    ax2 = fig.add_subplot(212)
    result_score.plot(ax=ax2, style='b-')
    ax2.legend(['score series'])

    # fig, axes = plt.subplots(2, 1)
    # print detect_series
    # # axes[0].plot(detect_series,style='k.-')
    # detect_series.plot(ax=axes[0], style='k.-')
    # plt.legend = ['original series']
    # plt.title(filename[i] + 'detect_compare_min200_max1440')
    # # axes[1].plot(result_score, style='b.-')
    # # axes[1].lenged = ['score series']
    # result_score.plot(ax=axes[1], style='b-')
    # plt.legend = ['score series']


    # plt.savefig(write_path+'/figure/'+filename[i]+'_min100_max1440.png', dpi=300)
    print i
plt.show()






