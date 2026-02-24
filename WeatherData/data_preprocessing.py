import os
import pandas as pd
from pathlib import Path

def main():
    folder_path = Path(__file__).parent / "Data"
    file_name = "Dades_XEMA_20260217.csv"

    df = pd.DataFrame()
    date_format = '%d/%m/%y %I:%M:%S %p'  #EDITED: Specify the date format of the downloaded data.
    # date_format = '%Y-%m-%d %H:%M:%S'  #EDITED: Specify the date format of the downloaded data.


    with open(folder_path / file_name, 'rt', encoding='utf-8') as f:

        df = pd.read_csv(f, sep=',', encoding='utf-8', header=0, engine='pyarrow')  #EDITED: Complete with the remaining parts of the read_csv function.

    # TODO - Complete the remaining parts of the script.
    df.DATA_LECTURA = pd.to_datetime(df['DATA_LECTURA'], format=date_format) #EDITED: Convert DATA_LECTURA to datetime
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

        56: 'WindGust_RatxaMaximaVent2m', #EDITED: Windgust Just in Caseç
        57: 'WindDirectionGust_2m', #EDITED: Map code 51

        53: 'WindGust_RatxaMaximaVent6m', #EDITED: Windgust Just in Case
        54: 'WindDirectionGust_6m', #EDITED: Map code 51
        72: 'RainMax', #Precipitacio maxima en 1 minut.

        }, inplace=True) #EDITED: Check information about the variable names in the useful information part.
    data_weather.to_csv(folder_path + 'meteo_processed.csv')
    
    
if __name__ == '__main__':
    main()
