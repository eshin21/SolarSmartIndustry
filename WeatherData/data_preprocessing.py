import os
import pandas as pd
from pathlib import Path

def main():
    folder_path = 'WeatherData/Data/'
    files_in_folder = os.listdir(folder_path)
    df = pd.DataFrame()
    date_format = '%d/%m/%Y %I:%M:%S %p'

    for file_name in files_in_folder:
        if not file_name.endswith('.csv'):
            continue

        else:
            with open(folder_path + file_name, 'rt', encoding='utf-8') as f:
                temp_df = pd.read_csv(f, sep=',', encoding='utf-8', header=0, engine='python')
            
                temp_df.DATA_LECTURA = pd.to_datetime(temp_df.DATA_LECTURA, format=date_format)
                temp_df.sort_values(by='DATA_LECTURA', inplace=True)
                temp_df.reset_index(inplace=True, drop=True)
            
                df = pd.concat([df, temp_df], ignore_index=True)


    # TODO - Complete the remaining parts of the script.
    df.DATA_LECTURA = pd.to_datetime(df['DATA_LECTURA'], format='mixed', dayfirst=True) #EDITED: Convert DATA_LECTURA to datetime
    df.sort_values(by='DATA_LECTURA', inplace=True) #EDITED: Sort by date
    df.reset_index(inplace=True, drop=True)

    data_weather = df.pivot(index='DATA_LECTURA', columns='CODI_VARIABLE', values='VALOR_LECTURA') #EDITED: Pivot table to get variables as columns
    data_weather.insert(0, 'DateTime', data_weather.index)
    data_weather.insert(1, 'CodiEstacio', df.CODI_ESTACIO[0])


    data_weather.rename(columns={
        30: 'Wind', #EDITED: Map code 30
        31: 'WindDirection', #EDITED: Map code 31
        32: 'Temperature', #EDITED: Map code 32
        33: 'RelativeHumidity', #EDITED: Map code 33
        34: 'Pressure', #EDITED: Map code 34
        35: 'Rain', #EDITED: Map code 35
        36: 'Irradiance', #EDITED: Map code 36
        40: 'TemperatureMax', #EDITED: Map code 40
        42: 'TemperatureMin', #EDITED: Map code 42


        44: 'RelativeHumidityMin', #EDITED: Map code 44
        3:  'RelativeHumidityMax', #EDITED: Map code 46

        50: 'WindGust',
        51: 'WindDirectionGust',
        72: 'RainMax', #Precipitacio maxima en 1 minut.

        }, inplace=True) #EDITED: Check information about the variable names in the useful information part.
    data_weather.to_csv(folder_path + 'meteo_processed.csv')
    
    
if __name__ == '__main__':
    main()
