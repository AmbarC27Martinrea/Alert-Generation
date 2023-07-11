import pymongo
import pandas as pd
import matplotlib.pyplot as plt 
from datetime import datetime, timedelta
from Press_21_threshold import *
from error_class import Error

# sensor_dict = {
#     "FL_IntrBrng_Vrms": [0.0003, 0.0070],
#     "FL_IntrBrng_Apeak": [0.8, 108.5],
#     "FL_IntrBrng_Arms": [0.2, 10.4],
#     "FL_IntrBrng_Crest": [3.4, 14.4],
#     "FL_IntrBrng_Temp": [92.1, 95.4],
#     "FRIntrBrng_Vrms": [0.0005, 0.0057],
#     "FRIntrBrng_Apeak": [1.0, 145.9],
#     "FRIntrBrng_Arms": [0.3, 12.7],
#     "FRIntrBrng_Temp": [92.48, 96.98],
#     "RLIntrBrng_Vrms": [0.0003, 0.0053],
#     "RLIntrBrng_Apeak": [0.5, 56.7],
#     "RLIntrBrng_Arms": [0.1, 4.4],
#     "RLIntrBrng_Crest": [3.6, 16.0],
#     "RLIntrBrng_Temp": [87.1, 89.4],
#     "RRIntrBrng_Vrms": [0.0002, 0.0071],
#     "RRIntrBrng_Apeak": [1.3, 102.0],
#     "RRIntrBrng_Arms": [0.3, 7.2],
#     "RRIntrBrng_Crest": [3.5, 17.7],
#     "RRIntrBrng_Temp": [87.8, 90.3],
#     "FL_EccBshng_Vrms": [0.0003, 0.0049],
#     "FL_EccBshng_Apeak": [0.4, 50.2],
#     "FL_EccBshng_Arms": [0.1, 4.4],
#     "FL_EccBshng_Crest": [3.2, 17.4],
#     "FL_EccBshng_Temp": [89.7, 94.0],
#     "FREccBshng_Vrms": [0.0003, 0.0032],
#     "FREccBshng_Apeak": [0.5, 89.5],
#     "FREccBshng_Arms": [0.1, 3.9],
#     "FREccBshng_Crest": [3.4, 32.1],
#     "FREccBshng_Temp": [91.9, 94.5],
#     "Prs_MMtrCltch_Vrms": [0.002, 0.012],
#     "Prs_MMtrCltch_Apeak": [11.8, 51.7],
#     "Prs_MMtrCltch_Arms": [3.1, 6.7],
#     "Prs_MMtrCltch_Crest": [3.6, 8.5],
#     "Prs_MMtrCltch_Temp": [86.3, 88.7],
#     "Prs_MMtr_Vrms": [0.0018, 0.0121],
#     "Prs_MMtr_Apeak": [19.4, 53.1],
#     "Prs_MMtr_Arms": [5.0, 7.8],
#     "Prs_MMtr_Crest": [3.6, 7.3],
#     "Prs_MMtr_Temp": [88.0, 90.2],
#     "Prs_Main_AirPre": [106.5, 110.9],
#     "CounterBalance_Pre": [28.1, 91.7],
#     "ClutchPre": [0, 68.0],
#     "Cl_Br_SrgTankPre": [59.1, 72.5],
#     "BrakePre": [0, 68.0],
#     "R_Cbal_RsrPre": [26.6, 90.2],
#     "L_CBal_RsrPre": [23.6, 85.8],
#     "Col1_Gibbs_Vrms": [0.0002, 0.0060],
#     "Col1_Gibbs_Apeak": [0.3, 22.0],
#     "Col1_Gibbs_Arms": [0.1, 3.6],
#     "Col1_Gibbs_Crest": [3.3, 8.7],
#     "Col1_Gibbs_Temp": [83.1, 84.2],
#     "Col2_Gibbs_Vrms": [0.0002, 0.0049],
#     "Col2_Gibbs_Apeak": [0.3, 19.4],
#     "Col2_Gibbs_Arms": [0.1, 3.1],
#     "Col2_Gibbs_Crest": [3.2, 8.8],
#     "Col2_Gibbs_Temp": [84.7, 86.0],
#     "Col3_Gibbs_Apeak": [0.3, 19.7],
#     "Col3_Gibbs_Arms": [0.1, 3.3],
#     "Col3_Gibbs_Crest": [3.3, 8.5],
#     "Col3_Gibbs_Temp": [83.4, 84.8],
#     "Col4_Gibbs_Apeak": [0.3, 19.1],
#     "Col4_Gibbs_Arms": [0.1, 3.2],
#     "Col4_Gibbs_Crest": [3.3, 8.6],
#     "Col4_Gibbs_Temp": [82.2, 83.7],
#     "Rm_LCNut_Vrms": [0.0002, 0.0126],
#     "Rm_LCNut_Apeak": [0.3, 23.8],
#     "Rm_LCNut_Arms": [0.1, 2.7],
#     "Rm_LCNut_Crest": [3.2, 14.9],
#     "Rm_LCNut_Temp": [86.2, 88.7],
#     "Rm_RCNut_Vrms": [0.0002, 0.0179],
#     "Rm_RCNut_Apeak": [0.3, 28.1],
#     "Rm_RCNut_Arms": [0.1, 3.5],
#     "Rm_RCNut_Crest": [2.9, 14.0],
#     "Rm_RCNut_Temp": [86.2, 88.9],
#     "Rm_AdjMtr_Vrms": [0.0005, 0.0365],
#     "Rm_AdjMtr_Arms": [0.2, 11.3],
#     "Rm_AdjMtr_Crest": [2.9, 13.5],
#     "Rm_AdjMtr_Temp": [83.8, 87.1],
#     "Rm_AdjBrng_Vrms": [0.0003, 0.0204],
#     "Rm_AdjBrng_Apeak": [0.4, 58.1],
#     "Rm_AdjBrng_Arms": [0.1, 7.6],
#     "Rm_AdjBrng_Crest": [3.0, 17.7],
#     "Rm_AdjBrng_Temp": [84.2, 86.7],
#     "Prs_LubePreFilterPre": [313, 362],
#     "Prs_LubePostFilterPre": [260, 309],
#     "Rm_LubeBlckInPre": [118, 160],
#     "Rm_LeftLwrLnk_Pre": [22, 31],
#     "Rm_RightLwrLnk_Pre": [19.2, 28.1],
#     "LubeBlock1Pre": [34.0, 62.1],
#     "LubeBlock2Pre": [120, 247],
#     "FrontLubeBlockPre": [100, 225],
#     "LubePump_Vrms": [0.0004, 0.0065],
#     "LubePump_Apeak": [5.6, 10.9],
#     "LubePump_Arms": [1.9, 2.4],
#     "LubePump_Crest": [2.6, 4.2],
#     "LubePump_Temp": [83.6, 85.5],
#     "LubeMotor_Vrms": [0.0003, 0.0056],
#     "LubeMotor_Apeak": [5.2, 9.3],
#     "LubeMotor_Arms": [1.9, 2.4],
#     "LubeMotor_Crest": [2.6, 4.2],
#     "LubeMotor_Temp": [95.1, 96.5]
# }
    
def press_21_alert_generator(start,end):
    press = "Press_21"

    sensor_dict = press_21_quantiles(start,end)

    myclient_global = pymongo.MongoClient(host = "128.121.34.13", connect = True )
    press_db = myclient_global[press]
    end = datetime.now()
    start   = end - timedelta(minutes=60)

    min_max = pd.DataFrame()

    for batch in ['BATCH_1','BATCH_2']:
        collection= press_db[batch]
        projection = {}
        projection['_id'] = 0
        projection['Date'] = 1

        for sensor in sensor_dict.keys():
            projection[sensor] = 1

        QUERY = {"Date": {'$gte': start, '$lt':  end}}
        results = collection.find(QUERY,projection)
        df1 = pd.DataFrame(results).set_index('Date')
        describe = df1.describe().T
        describe = describe[['min','max']]
        min_max = pd.concat([min_max,describe],axis=0)

    print(min_max)

    error_list = []
    for i in range(len(min_max)):
        name = min_max.iloc[i].name
        lower = min_max.iloc[i].min()
        upper = min_max.iloc[i].max()
        [lower_limit, upper_limit] = sensor_dict[name]

        if lower < lower_limit and upper > upper_limit:
            lower_error = True
            upper_error = True
        elif lower < lower_limit:
            lower_error = True
            upper_error = False
        elif upper > upper_limit:
            lower_error = False
            upper_error = True
        else:
            lower_error = False
            upper_error = False

        if lower_error or upper_error:
            descr = ""
            if lower_error and upper_error:
                descr = "Sensor registered data both below limit and above upper limit in the last 5 minutes"
            elif lower_error:
                descr = "Sensor registered data below lower limit"
            elif upper_error:
                descr = "Sensor registered data above upper limit"
            error = Error(name,descr)
            error_list.append(error)

    return error_list

print(press_21_alert_generator(datetime(2023,6,13,10,0,0),datetime(2023,6,13,11,0,0)))