import os
import pandas as pd
import re
from tqdm import tqdm
from config import *
from toolkit import *

if __name__ == "__main__":
    fileName_list = os.listdir(Data_Folder_Path)
    stationInfo_list = pd.read_csv(Station_Info_Path)

    # 筛选可用台站：选取同时有地声、电磁数据且在eqlst.csv内标记为未来可用的台站
    _continueable_stations = stationInfo_list[stationInfo_list['MagnUpdate'] & stationInfo_list['SoundUpdate']][
        'StationID'].unique()
    _continueable_stations = set(_continueable_stations)
    re_magn = re.compile(r'(\d+)_magn.csv')
    re_sound = re.compile(r'(\d+)_sound.csv')
    _set_magn = set()
    _set_sound = set()
    for filename in fileName_list:
        _magn_match = re_magn.findall(filename)
        _sound_match = re_sound.findall(filename)
        if (_magn_match):
            _set_magn.add(int(_magn_match[0]))
            continue
        if (_sound_match):
            _set_sound.add(int(_sound_match[0]))
            continue
    # 查并集：有效站点 且该站点有地声数据 且该站点有电磁数据
    usable_stations = _continueable_stations & _set_magn & _set_sound
    # 保存有效站点列表（序列化）
    dump_object(Usable_Station_Path, usable_stations)

    print('合并数据:')
    for type in ('magn', 'sound'):
        res = []
        for _id in tqdm(usable_stations, desc=f'{type}:'):
            _df = pd.read_csv(Data_Folder_Path + str(_id) + f'_{type}.csv')[Used_features[type]]
            res.append(_df)
        # 将所有同类型数据放入一个df中
        final_df = pd.concat(res)
        # 持久化
        final_df.to_pickle(Merged_Data_Path[type])
        del (final_df)
