import pymongo
import pandas as pd
import matplotlib.pyplot as plt 
from datetime import datetime, timedelta

def press_21_quantile(category,start,end):

    if category in ["Bolster","Press","Ram"]:
        batch = 'BATCH_1'
    elif category in ["Crown","Lubrication"]:
        batch = 'BATCH_2'
    else:
        raise('Category not valid')

    df = pd.read_excel(r"C:\Users\achakraborty\Downloads\P21_Live_DataPointsList.xlsx",sheet_name = 'Sheet1')
    tag_name = df['Tag Name']
    tag_name = list(tag_name)
    sensor_class = df['Class']
    sensor_class = list(sensor_class)

    tags = []
    for i in range(len(sensor_class)):
        if sensor_class[i] == category:
            tags.append(tag_name[i])

    print(len(tags))

    press = "Press_21"

    myclient_global = pymongo.MongoClient(host = "128.121.34.13", connect = True )
    press_db = myclient_global[press]
    collection= press_db[batch]
    
    earliest_date = collection.find_one({}, {"Date": 1}, sort=[("Date", 1)])['Date']
    # start = datetime(2023,6,14,0,0,0)
    # end   = start + timedelta(hours=21)

    projection = {}
    projection['_id'] = 0
    projection['Date'] = 1
    for field in tags:
        projection[field] = 1

    print(len(projection))
    print(projection)


    QUERY = {"Date": {'$gte': start, '$lt':  end}}
    results = collection.find(QUERY,projection)
    df1 = pd.DataFrame(results).set_index('Date')
    describe = df1.describe().T

    quantile = df1.quantile([0.005,0.995], axis = 0)
    return quantile.T
    # # quantile = quantile.drop('Date', axis='columns')
    # quantiles = pd.concat([quantiles, quantile], axis=1)

    # print(quantiles.T)

    # for i in range(len(df1.columns)):
    #     plt.plot(df1.iloc[:,i],color='black')
    #     plt.ylabel(df1.columns[i])
    #     plt.xlabel("Date")
    #     if df1.columns[i].endswith('Vrms'):
    #         plt.ylim(0,0.01)
    #     elif df1.columns[i].endswith('Arms'):
    #         plt.ylim(0,10)
    #     elif df1.columns[i].endswith('Temp'):
    #         plt.ylim(70,100)
    #     elif df1.columns[i].endswith('Apeak'):
    #         plt.ylim(0,250)
    #     elif df1.columns[i].endswith('Crest'):
    #         plt.ylim(0,50)
    #     plt.xticks(rotation=90)
    #     plt.show()

def press_21_quantiles(start,end):
    quantiles = pd.DataFrame()
    categories = ['Crown','Ram','Lubrication','Bolster','Press']
    for category in categories:
        quantile = press_21_quantile(category,start,end)
        quantiles = pd.concat([quantiles,quantile],axis=0)
    # press_plotter_21('Crown')
    # press_21_quantile('Ram')
    # press_plotter_21('Lubrication')
    # press_plotter_21('Bolster')
    # press_plotter_21('Press')

    sensor_dict = {}
    for i in range(len(quantiles)):
        index = quantiles.index.to_list()[i]
        thresholds = quantiles.iloc[i].to_list()
        sensor_dict[index] = thresholds

    return sensor_dict ## this dictionary gives the threshold values

print(press_21_quantiles(datetime(2023,6,13,10,0,0),datetime(2023,6,13,11,0,0)))