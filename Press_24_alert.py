import pymongo
import pandas as pd
import matplotlib.pyplot as plt 
from datetime import datetime, timedelta
from Press_24_threshold import *
from error_class import Error
    
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
        describe = df1.quantile([0.005,0.995], axis = 0)
        # describe = describe[['min','max']]
        min_max = pd.concat([min_max,describe],axis=1)

    # print(min_max)

    error_list = []
    for i in range(len(min_max)):
        name = min_max.columns[i]
        lower = min_max.iloc[i,0]
        upper = min_max.iloc[i,1]
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

# print(press_24_alert_generator(datetime(2023,6,13,10,0,0),datetime(2023,6,13,11,0,0)))