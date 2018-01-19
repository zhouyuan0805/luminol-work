#coding=utf-8
import pandas as pd
import time
from luminol.anomaly_detector import AnomalyDetector

def timestamp_to_datetime(x):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x))

def score_to_df(score_data):
    temp =[]
    for timestamp, value in score.iteritems():
        temp.append([timestamp_to_datetime(timestamp),value])
    temp_df = pd.DataFrame(temp, columns=['kpi_time','kpi_value'])
    return temp_df

read = pd.read_csv('/root/***.csv', delimiter='\t')

read_df = pd.DataFrame(read)
read_df.ix[:,0] = read_df.ix[:,0].apply(timestamp_to_datetime)

dic_data = {}
for i in range(len(read)):
    dic_data[read.ix[i,0]] = read.ix[i,1]

detector = AnomalyDetector(dic_data)
score = detector.get_all_scores()
score_df = score_to_df(score)

score_df.to_csv('/root/***_score.csv',index=False, header=False)
read_df.to_csv('/root/***_date.csv',index=False, header=False)