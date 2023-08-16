from Press_11_threshold import press_11_quantiles
from Press_21_threshold import press_21_quantiles
from Press_24_threshold import press_24_quantiles
from datetime import datetime
import pandas as pd

def task1():
    while True:
        try:
            press = int(input("Which press do you want to analyze? Input only the number of the press: "))
            print(" ")
            sheet = f'Press_{press}_production_details'
            df = pd.read_excel('live_sensors_list (3).xlsx',sheet_name = sheet)
            break
        except ValueError:
            print('Invalid Press. Try again')
    for i in range(len(df.columns.to_list())):
        print(f'{i+1}: {df.columns.to_list()[i]}')
    while True:
        try:
            job = int(input("Enter index number of job you want to analyze: "))
            print(" ")
            if job == 0:
                raise(IndexError)
            production_times = df.iloc[:,job-1].dropna().to_list()
            break
        except IndexError:
            print('Invalid index, try again')
    for i in range(len(production_times)):
        print(f'{i+1}: {production_times[i]}')
    while True:
        try:
            time_index = int(input('Enter index of time you want to analyze: ')) - 1
            print(" ")
            if time_index < 0 or time_index >= len(production_times):
                raise(IndexError)
            time = production_times[time_index].split(",")
            break
        except (IndexError):
            print('Invalid index, try again')
    for i in range(len(time)):
        time[i] = int(time[i])
    production_time = [[datetime(time[0],time[1],time[2],time[3],time[4],time[5]),datetime(time[6],time[7],time[8],time[9],time[10],time[11])]]
    while True:
        try:
            select_values = int(input("Press 0 for getting all the sensor values. Press 1 for getting specific sensor values "))
            while True:
                try:
                    if select_values not in [0,1]:
                        raise(IndexError)
                    break
                except IndexError:
                    print('Input either 0 or 1')
            if select_values == 1:
                sensor_string = input("Input sensors for which you want the data, separated by a comma. Don't include any whitespaces anywhere: ")
                if "," in sensor_string:
                    sensor_list = sensor_string.split(",")
                else:
                    sensor_list = [sensor_string]
            if press == 11:
                ans_df = press_11_quantiles(production_time)
            elif press == 21:
                ans_df = press_21_quantiles(production_time)
            elif press == 24:
                ans_df = press_24_quantiles(production_time)
            
            if select_values == 1:
                ans_df = ans_df.T
                df = pd.DataFrame()
                for sensor in sensor_list:
                    df = pd.concat([df,ans_df[sensor]],axis=1)
                print(df.T)
            else:
                print(ans_df)
        except KeyError:
            print('Input either 0 or 1')

if __name__ == '__main__':
    task1()