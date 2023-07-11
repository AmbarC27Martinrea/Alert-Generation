import pymongo
import pandas as pd
import matplotlib.pyplot as plt 
from datetime import datetime, timedelta
from Press_24_threshold import *

# sensor_dict = {'DieClamp1_Pre': [0.0, 3000.688], 'DieClamp2_Pre': [0.0, 2999.209], 'QDC_UnClamp_Pre': [2.9578, 7.3945], 'QDC_Clamp_Pre': [0.0, 4.4367], 'QDC_Lift_Pre': [0.0, 0.0], 'MainHPU_Psressure_Pre': [2826.178, 2994.772], 'Pnmatic_Driving_Pre': [54.7193, 56.1982], 'FL_UprLink_Pin_Vrms': [0.0, 0.002], 'FL_UprLink_Pin_Apk': [0.2, 44.2], 'FL_UprLink_Pin_Arms': [0.0, 1.8], 'FL_UprLink_Pin_Crst': [3.5, 37.6], 'FL_UprLink_Pin_Temp': [89.6, 95.18001], 'FL_Eccntric_Pin_Vrms': [0.0, 0.0022], 'FL_Eccntric_Pin_Apk': [0.3, 67.2], 'FL_Eccntric_Pin_Arms': [0.1, 2.7], 'FL_Eccntric_Pin_Crst': [2.7, 40.2], 'FL_Eccntric_Pin_TempFar': [90.68001, 99.14], 'Frnt_InterPin_Vrms': [0.0001, 0.0043], 'Frnt_InterPin_Apk': [0.2, 86.0], 'Frnt_InterPin_Arms': [0.0, 2.8], 'Frnt_InterPin_Crst': [3.1, 33.9], 'Frnt_InterPin_Temp': [88.16, 93.2], 'Drive_Shaft_Pin_Vrms': [0.0, 0.0039], 'Drive_Shaft_Pin_Apk': [0.4, 125.7], 'Drive_Shaft_Pin_Arms': [0.1, 5.6], 'Drive_Shaft_Pin_Crst': [2.0, 43.4], 'Drive_Shaft_Pin_Temp': [90.32001, 98.6], 'FR_Eccntric_Pin_Vrms': [0.0, 0.0035], 'FR_Eccntric_Pin_Apk': [0.3, 69.9], 'FR_Eccntric_Pin_Arms': [0.1, 3.6], 'FR_Eccntric_Pin_Crst': [2.7, 40.8], 'FR_Eccntric_Pin_Temp': [89.24001, 97.34], 'FR_UprLink_Pin_Vrms': [0.0, 0.0027], 'FR_UprLink_Pin_Apk': [0.2, 107.7], 'FR_UprLink_Pin_Arms': [0.0, 3.0], 'FR_UprLink_Pin_Crst': [2.7, 38.8], 'FR_UprLink_Pin_Temp': [88.88, 93.38], 'RR_EccntricBshg_Vrms': [0.0, 0.0032], 'RR_EccntricBshg_Apk': [0.2, 17.0], 'RR_EccntricBshg_Arms': [0.1, 1.5], 'RR_EccntricBshg_Crst': [3.0, 23.9], 'RR_EccntricBshg_Temp': [89.6, 94.82001], 'RR_UprLink_Bshg_Vrms': [0.0, 0.0022], 'RR_UprLink_Bshg_Apk': [0.2, 92.62000000001862], 'RR_UprLink_Bshg_Arms': [0.1, 2.7], 'RR_UprLink_Bshg_Crst': [3.2, 40.0], 'RR_UprLink_Bshg_Temp': [89.42, 93.56], 'FlyWhlShaftBshg_Outside_Vrms': [0.0001, 0.0039], 'FlyWhlShaftBshg_Outside_Apk': [0.9, 37.2], 'FlyWhlShaftBshg_Outside_Arms': [0.2, 2.4], 'FlyWhlShaftBshg_Outside_Crst': [2.6, 17.4], 'FlyWhlShaftBshg_Outside_Temp': [98.42001, 103.82], 'RL_EccntricBshg_Vrms': [0.0, 0.0022], 'RL_EccntricBshg_Apk': [0.3, 37.9], 'RL_EccntricBshg_Arms': [0.1, 2.4], 'RL_EccntricBshg_Crst': [2.8, 36.9], 'RL_EccntricBshg_Temp': [89.06, 96.8], 'RL_UprLink_Bshg_Vrms': [0.0, 0.003], 'RL_UprLink_Bshg_Apk': [0.2, 76.4], 'RL_UprLink_Bshg_Arms': [0.1, 2.9], 'RL_UprLink_Bshg_Crst': [1.8, 32.3], 'RL_UprLink_Bshg_Temp': [86.9, 91.58], 'uncolr_Mandrel_ret_Pre': [0.0, 31.0569], 'uncolr_Mandrel_Exp_Pre': [837.0574, 1082.555], 'uncolr_Mandrel_JgFwd_Pre': [13.3101, 718.7454], 'uncolr_Mandrel_JgRev_Pre': [1.4789, 845.9308], 'uncolr_ClutchnBrake_Pre': [0.0, 0.0], 'Fdr_Pnch_Rler_Pnmatic_Pre': [11.8312, 16.2679], 'Fdr_UpDwn_Pnmatic_Pre': [17.7468, 17.7468], 'Fdr_Guide_Pre': [90.2129, 110.9175], 'Stnr_Pmp_HydPre': [635.927, 983.4685], 'Stnr_Pmp_Vrms': [0.0016, 0.003], 'Stnr_Pmp_Apk': [24.6, 150.7], 'Stnr_Pmp_Arms': [6.2, 19.9], 'Stnr_Pmp_Crst': [3.5, 10.3], 'Stnr_Pmp_Temp': [99.32, 112.82], 'Stnr_HydResvr_OilLvl': [100.0, 32760.0], 'Stnr_MandrelArmRoller_FwdPre': [409.6553, 585.6444], 'Stnr_MandrelArmRoller_RevPre': [100.5652, 143.4533], 'Stnr_HydMtrVrms': [0.0001, 0.0014], 'Stnr_HydMtrApk': [1.3, 71.2], 'Stnr_HydMtrArms': [0.4, 7.2], 'Stnr_HydMtrCrst': [3.0, 16.9], 'Stnr_HydMtrTemp': [82.22, 92.48], 'Stnr_Pinch_Roller_Pre': [0.0, 22.1835], 'Stnr_Mtr_Vrms': [0.0009, 0.0021], 'Stnr_Mtr_Apk': [14.7, 30.0], 'Stnr_Mtr_Arms': [4.2, 6.2], 'Stnr_Mtr_Crst': [3.3, 5.6], 'Stnr_Mtr_Temp': [105.08, 116.06], 'RadMtr_Vrms': [0.0001, 0.0063], 'RadMtr_Apk': [0.3, 11.5], 'RadMtr_Arms': [0.1, 2.4], 'RadMtr_Crst': [3.2, 5.3], 'RadMtr_Temp': [83.84, 90.86], 'Crwn_HydUnit_Resvr_OilLvl': [122.0, 160.0], 'Crwn_HydUnit_HydClutch_Pre': [14.789, 1110.654], 'Crwn_HydUnit_HydraulicPre': [19.2257, 1113.612], 'Crwn_HydUnit_HydMtrVrms': [0.0003, 0.015], 'Crwn_HydUnit_HydMtrApk': [0.3, 27.7], 'Crwn_HydUnit_HydMtrArms': [0.1, 6.7], 'Crwn_HydUnit_HydMtrCrst': [3.2, 4.5], 'Crwn_HydUnit_HydMtrTemp': [86.9, 91.76], 'Crwn_HydUnit_HydPmp_Vrms': [0.0003, 0.0095], 'Crwn_HydUnit_HydPmp_Apk': [0.3, 59.8], 'Crwn_HydUnit_HydPmp_Arms': [0.1, 13.6], 'Crwn_HydUnit_HydPmp_Crst': [3.3, 5.0], 'Crwn_HydUnit_HydPmp_Temp': [104.18, 110.66], 'Ram_FR_CNut_Lube_Pre': [0.0, 115.3542], 'Ram_FL_CNut_Lube_Pre': [2.9578, 125.7065], 'Ram_RR_CNut_Lube_Pre': [0.0, 119.7909], 'Ram_RL_CNut_Lube_Pre': [0.0, 128.6643], 'Rear_LubeBlock_Pre': [0.0, 388.9507], 'Pit_Hyd_Prefilter_Real_PSI': [607.8279, 736.4922], 'Pit_HydPre_Postfilter_Real_PSI': [600.4334, 729.0977], 'Pit_HydOverload_Pre_Real_PSI': [1734.75, 1891.513], 'Pit_Pmp_Vrms': [0.0003, 0.0008], 'Pit_Pmp_Apk': [12.9, 35.6], 'Pit_Pmp_Arms': [3.0, 4.7], 'Pit_Pmp_Crst': [4.2, 8.1], 'Pit_Pmp_Temp': [83.66, 86.0], 'Pit_LubeLevel': [211.0, 324.0], 'Pit_Mtr_Vrms': [0.0002, 0.0006], 'Pit_Mtr_Apk': [4.8, 7.7], 'Pit_Mtr_Arms': [1.3, 1.6], 'Pit_Mtr_Crst': [3.5, 5.3], 'Pit_Mtr_Temp': [76.46001, 79.52], 'PitFlood': [0.0, 0.0], 'CBal_pre_PSI': [70.9872, 93.1707], 'QDC_Drive_Pre_PSI': [88.734, 109.4386], 'RCBal_Resvr_Pre': [69.5083, 93.1707], 'LCBal_Resvr_Pre_PSI': [70.9872, 93.1707], 'FL_Gb_Cnd_Vrms': [0.0, 0.0021], 'FL_Gb_Cnd_Apk': [0.2, 13.3], 'FL_Gb_Cnd_Arms': [0.0, 1.5], 'FL_Gb_Cnd_Crst': [3.4, 11.6], 'FL_Gb_Cnd_Temp': [79.7, 83.66], 'FR_Gb_Cnd_Vrms': [0.0, 0.0024], 'FR_Gb_Cnd_Apk': [0.2, 12.9], 'FR_Gb_Cnd_Arms': [0.1, 1.4], 'FR_Gb_Cnd_Crst': [3.4, 10.5], 'FR_Gb_Cnd_Temp': [81.14, 86.54], 'RR_Gb_Cnd_Apk': [0.2, 11.0], 'RR_Gb_Cnd_Arms': [0.1, 1.4], 'RR_Gb_Cnd_Vrms': [0.0, 0.0019], 'RR_Gb_Cnd_Crst': [3.5, 11.6], 'RR_Gb_Cnd_Temp': [79.34, 84.38], 'RL_Gb_Cnd_Vrms': [0.0, 0.0023], 'RL_Gb_Cnd_Apk': [0.2, 12.1], 'RL_Gb_Cnd_Arms': [0.1, 1.2], 'RL_Gb_Cnd_Crst': [3.0, 13.9], 'RL_Gb_Cnd_Temp': [82.22, 88.52], 'Ram_FR_CNut_Vrms': [0.0001, 0.0104], 'Ram_FR_CNut_Apk': [0.2, 49.51949999998324], 'Ram_FR_CNut_Arms': [0.1, 2.8], 'Ram_FR_CNut_Crst': [3.5, 37.8], 'Ram_FR_CNut_Temp': [86.0, 89.96001], 'Ram_FL_CNut_Vrms': [0.0, 0.009699999], 'Ram_FL_CNut_Apk': [0.2, 16.1], 'Ram_FL_CNut_Arms': [0.0, 2.7], 'Ram_FL_CNut_Crst': [3.4, 17.4], 'Ram_FL_CNut_Temp': [84.74001, 89.06], 'Ram_RR_CNut_Vrms': [0.0, 0.0105], 'Ram_RR_CNut_Apk': [0.2, 38.7], 'Ram_RR_CNut_Arms': [0.0, 2.6], 'Ram_RR_CNut_Crst': [3.5, 28.0], 'Ram_RR_CNut_Temp': [83.12, 87.44], 'Ram_RL_CNut_Vrms': [0.0, 0.009699999], 'Ram_RL_CNut_Apk': [0.2, 51.9], 'Ram_RL_CNut_Arms': [0.0, 2.7], 'Ram_RL_CNut_Crst': [3.4, 45.8], 'Ram_RL_CNut_Temp': [83.66, 88.52], 'Ram_Adj_Mtr_Vrms': [0.0001, 0.0153], 'Ram_Adj_Mtr_Apk': [0.2, 20.5], 'Ram_Adj_Mtr_Arms': [0.1, 4.7], 'Ram_Adj_Mtr_Crest': [3.2, 10.8], 'Ram_Adj_Mtr_Temp': [82.22, 89.06], 'Xfer_MainPre_PSI': [88.734, 109.4386], 'Xfer_HydUnit_OilLvl_UGT526inmm': [120.0, 152.0], 'Xfer_UnitHydMtr_Vrms': [0.0, 0.0017], 'Xfer_UnitHydMtr_Apk': [0.2, 6.0], 'Xfer_UnitHydMtr_Arms': [0.0, 1.5], 'Xfer_UnitHydMtr_Crst': [3.3, 6.0], 'Xfer_UnitHydMtr_Temp': [82.04, 110.3], 'XferHyd_Pre': [4.4367, 971.6373]}

class Error:
    def __init__(self,sensor_name,description):
        self.sensor_name = sensor_name
        self.description = description

    def __repr__(self):
        return f"{self.sensor_name}: {self.description}"
    
def press_24_alert_generator(start,end):
    press = "Press_24"

    sensor_dict = press_24_quantiles(start,end)

    myclient_global = pymongo.MongoClient(host = "128.121.34.13", connect = True )
    press_db = myclient_global[press]
    end = datetime.now()
    start   = end - timedelta(minutes=60)

    min_max = pd.DataFrame()

    for batch in ['BATCH_1','BATCH_2','BATCH_3','BATCH_4']:
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

print(press_24_alert_generator(datetime(2023,6,13,10,0,0),datetime(2023,6,13,11,0,0)))