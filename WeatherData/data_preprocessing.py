import os
import pandas as pd


def main():
    folder_path = 'Data/'
    files_in_folder = os.listdir(folder_path)

    df = pd.DataFrame()
    date_format = (...)  # TODO Specify the date format of the downloaded data.

    for file_name in files_in_folder:
        with open(folder_path + file_name, 'rt', encoding='utf-8') as f:
            df = pd.read_csv(f, sep=(...), encoding=(...), header=(...), engine=(...))  # TODO Complete with the remaining parts of the read_csv function.

    # TODO - Complete the remaining parts of the script.
    df.DATA_LECTURA = pd.to_datetime((...), format=(...))
    df.sort_values(by=(...), inplace=True)
    df.reset_index(inplace=True, drop=True)

    data_weather = (...) # TODO - df.pivot function might be useful
    data_weather.insert(0, 'DateTime', data_weather.index)
    data_weather.insert(1, 'CodiEstacio', df.CODI_ESTACIO[0])
    data_weather.rename(columns={
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...),
        (...): (...)}, inplace=True) # TODO - Check information about the variable names in the useful information part.
    data_weather.to_csv(folder_path + 'meteo_processed.csv')
    
    
if __name__ == '__main__':
    main()

