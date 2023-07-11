import pymongo
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
from datetime import datetime, timedelta

def press_11_quantile(category,start,end):
    press = "Press_11"

    myclient_global = pymongo.MongoClient(host = "128.121.34.13",connect = True)
    press_db = myclient_global[press]

    df = pd.read_excel(r"C:\Users\achakraborty\Downloads\live_sensors_list (2).xlsx",sheet_name = 'Press_11')
    tag_name = df['Tag Name']
    tag_name = list(tag_name)
    sensor_class = df['Class']
    sensor_class = list(sensor_class)

    print(len(sensor_class),len(tag_name))

    tags = []
    for i in range(len(sensor_class)):
        if sensor_class[i] == category:
            tags.append(tag_name[i])

    print(tags)
    print(len(tags))
    print(tag_name)

    batch_info_dict = {} #empty dict

    for column in tags:
        for batch in [1,2]:
            collection = "BATCH_"+str(batch) #collection in db
            col = press_db[collection]
            results = col.find().limit(1).sort("_id",-1) # getting the first entry in db
            recent_doc = results[0] #getting the first entry in db
            if column in recent_doc.keys():
                if batch not in batch_info_dict:
                    batch_info_dict[batch] = [column]
                else:
                    batch_info_dict[batch].append(column)
    print(batch_info_dict)
    batch_info_dict.keys()

    tot = 0
    for batch in batch_info_dict:
        tot += len(batch_info_dict[batch])
    print(tot)

    quantiles = pd.DataFrame()

    for key in batch_info_dict.keys():
        batch = "BATCH_" + str(key)
        collection= press_db[batch]

        fields = batch_info_dict[key]

        earliest_date = collection.find_one({}, {"Date": 1}, sort=[("Date", 1)])['Date']
        print(earliest_date)

        # start = datetime.strptime(earliest_date, "%d-%b-%Y %H:%M:%S.%f") + timedelta(days=15)
        # start = datetime(2023,6,13,0,0,0)
        # end   = start + timedelta(hours=12)
        

        projection = {}
        projection['_id'] = 0
        projection['Date'] = 1
        projection['Press_angle'] = 1
        for field in fields:
            projection[field] = 1

        print(len(projection),projection)

        QUERY = {'$and':[{"Date": {'$gte': start, '$lt':  end}},{"Press_angle" : {'$gte':0, '$lte': 200}}]}
        results = collection.find(QUERY,projection)
        df1 = pd.DataFrame(results).set_index('Date')
        df1.reset_index(inplace=True)
        df1.index = range(1, len(df1) + 1)
        describe = df1.describe().T

        def filtered(df,minutes=40*1*60):
            '''
            By default function filters out every minute when press isn't running (40 datapoints per second times 60 seconds per minute)
            This can be changed by specifying minute variable

            Example:
            filtered(df) would filter data for every minute press isn't running
            filtred(df,5) would filter data for every 5 minutes press isn't running
            '''
            
            # Split the dataframe into smaller chunks
            df_chunks = [df[i:i+minutes] for i in range(0, len(df), minutes)]

            # Filter out chunks with zero standard deviation in 'Press_Angle'
            filtered_chunks = [chunk for chunk in df_chunks if np.std(chunk['Press_angle']) != 0]

            filtered_df = pd.concat(filtered_chunks, ignore_index=True)

            return filtered_df
        
        filtered_df = filtered(df1)

        result = []  # List to store the percentiles

        quantile = np.percentile(filtered_df.values,[0.5,99.5],axis=0)
        # quantile = quantile.drop('Date', axis='columns')
        print(quantile)
        quantile = pd.DataFrame(quantile, columns=filtered_df.columns, index=[5,95])
        quantiles = pd.concat([quantiles, quantile], axis=1)
            

        # for i in range(len(df1.columns)):
        #     if df1.columns[i].endswith('Arms'):
        #         plt.plot(filtered_df.iloc[:,i],color='black')
        #         plt.ylabel(df1.columns[i])
        #         plt.xlabel("Date")
        #         plt.xticks(rotation=90)
        #         plt.ylim(0,3)
        #         plt.show()
        #     elif df1.columns[i].endswith('Vrms'):
        #         plt.plot(filtered_df.iloc[:,i],color='black')
        #         plt.ylabel(df1.columns[i])
        #         plt.xlabel("Date")
        #         plt.xticks(rotation=90)
        #         plt.ylim(0,0.01)
        #         plt.show()
        #     else:
        #         plt.plot(filtered_df.iloc[:,i],color='black')
        #         plt.ylabel(df1.columns[i])
        #         plt.xlabel("Date")
        #         plt.xticks(rotation=90)
        #         plt.show()

    return quantiles.T

def press_11_quantiles(start,end):
    quantiles = pd.DataFrame()
    categories = ['Crown','Ram']
    for category in categories:
        quantile = press_11_quantile(category,start,end)
        quantile = quantile.drop(quantile.index[0])
        quantiles = pd.concat([quantiles,quantile],axis=0)
    # print(quantiles)

    sensor_dict = {}
    for i in range(len(quantiles)):
        index = quantiles.index.to_list()[i]
        thresholds = quantiles.iloc[i].to_list()
        sensor_dict[index] = thresholds

    return sensor_dict ## this dictionary gives the threshold values

# print(press_11_quantiles(datetime(2023,6,13,10,0,0),datetime(2023,6,13,11,0,0)))