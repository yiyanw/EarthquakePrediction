import time, datetime
import pandas as pd
import pylab
import matplotlib.pyplot as plt
import time
from pathlib import Path
from sklearn import preprocessing
import numpy as np
import os
import logging
import sys


def make_dir(make_dir_path):
    path = make_dir_path.strip()
    if not os.path.exists(path):
        os.makedirs(path)
    return path


make_dir("./log")
logging.basicConfig()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
fh = logging.FileHandler('./log/down_sample.log', mode='a', encoding='utf-8')
fh.setFormatter(formatter)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)
logger.setLevel(logging.DEBUG)


# 获取时间戳
def getTimeStampByDate(date):
    timeArray = time.strptime(date, "%Y-%m-%d %H:%M:%S")
    return int(time.mktime(timeArray))


# 添加feature max min std mean ....
def addComputedFeatures(curDataList, targetDataList):
    desamlped_sound_data = curDataList.describe().iloc[2:8, 2:]
    for col in desamlped_sound_data.columns.values:
        for idx in desamlped_sound_data.index:
            targetDataList[str(col) + "_" + str(idx)] = [desamlped_sound_data.at[idx, col]]

    return targetDataList


# 降采样
def groupDataByDay(data):
    # 将一天的数据平均化
    # 获取2017-01-01 00:00:00的时间戳
    data = data.reset_index()
    startOfCurDay = getTimeStampByDate('2018-01-01 00:00:00')
    oneDay = 60 * 60 * 24
    resultDF = pd.DataFrame(columns=data.columns)
    curDataList = pd.DataFrame(columns=data.columns)
    for (idx, curStamp) in zip(data["index"], data["TimeStamp"]):
        while curStamp > (startOfCurDay + oneDay):
            if curDataList.shape[0] != 0:
                tmpDf = pd.DataFrame(curDataList.mean()).T
                tmpDf.loc[0, "TimeStamp"] = startOfCurDay
                timeArray = time.localtime(tmpDf.loc[0, "TimeStamp"])
                formatedTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
                tmpDf.insert(2, "date", formatedTime)
                tmpDf = addComputedFeatures(curDataList, tmpDf)
                resultDF = pd.merge(resultDF, tmpDf, how='outer')
            curDataList = pd.DataFrame(columns=data.columns)
            startOfCurDay = startOfCurDay + oneDay
        curDataList = curDataList.append(pd.DataFrame(data.loc[idx]).T)

    return resultDF.drop(["index"], axis=1)


import multiprocessing as mp


def func(filename, i, parent_path):
    try:
        original_data = pd.read_csv(filename)
        logger.info("finish reading file" + filename + " ...");
        desampled_data = groupDataByDay(original_data)
        logger.info("finish desample df {} ...".format(i));
        desampled_data.to_csv(parent_path + "/desampled/" + str(i) + "_desampled_sound.csv", index=False, header=True)
        logger.info("finish writing file {} ...".format(i))
    except Exception as e:
        print(e)


if __name__ == '__main__':
    logger.info(mp.cpu_count())

    pool = mp.Pool(9)

    args_list = []
    for i in range(9):
        args_list.append(("./AETA/training/sound/merged/" + str(i) + "_merged_sound.csv",
                          i,
                          "./training/sound"))  # 不要用parent_path那个var，因为pool不支持读取global variable
        # 这个path就是 /desampled/....前面的所有path，你们的大概率是 ./training/sound
    for i in range(9):
        args_list.append(("./AETA/training/magn/merged/" + str(i) + "_merged_magn.csv",
                          i,
                          "./training/magn"))  # 不要用parent_path那个var，因为pool不支持读取global variable
        # 这个path就是 /desampled/....前面的所有path，你们的大概率是 ./training/magn

    pool.starmap(func, args_list)
