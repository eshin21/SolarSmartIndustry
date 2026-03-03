import time
import pvlib
import datetime
import pandas as pd
import influx_config #EDITED: Import influx config
import pv_system_config #EDITED: Import pv system config
import os 
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from pvlib.location import Location
from pvlib.pvsystem import PVSystem, Array, FixedMount

execution_period = 5

def _get_specific_data(full_data, current_time, prev_time_data, prev_data):
    specific_data = None

    if prev_time_data is None:
        prev_time_data = current_time.replace(hour=current_time.hour - 1)

    if full_data is not None and not full_data.empty:
        # Map current time to the year of the available data
        data_year = full_data.index[0].year
        search_time = current_time.replace(year=data_year, tzinfo=None)

    # TODO - Return closest data, in 30min intervals, to the requested date after prev_data
        idx = full_data.index.get_indexer([search_time], method='nearest')[0]
        # TODO - Specific data must be a dictionary with column name as key and the corresponding value as value

        specific_data = full_data.iloc[idx].to_dict()

    return specific_data, prev_time_data

def _get_pv_structure(scenario_configuration, pv_module, modules_line, columns_array, name_array):
    scenario_mount = FixedMount(surface_tilt=scenario_configuration['tilt'],
                                surface_azimuth=scenario_configuration['orientation'],
                                module_height=scenario_configuration['altitude'])
    scenario_array = Array(mount=scenario_mount, module=pv_module['name'],
                           modules_per_string=modules_line, strings=columns_array, name=name_array)
    return scenario_array


def _get_effective_irradiance(scenario_config, solar_position, meteo_data):
    # TODO - Substitute ... to the name of the column that contains Irradiance
    if meteo_data['Irradiance'] != 0: 
        aoi_scenario = pvlib.irradiance.aoi(surface_tilt=scenario_config['tilt'], surface_azimuth=scenario_config['orientation'],
                                            solar_zenith=solar_position.apparent_zenith, solar_azimuth=solar_position.azimuth)
        iam_scenario = pvlib.iam.ashrae(aoi=aoi_scenario)
        effective_irradiance = meteo_data['Irradiance']*iam_scenario 

    else:
        effective_irradiance = meteo_data['Irradiance']
    return effective_irradiance



def _get_temperature_cell(meteo_data):
    temp_cell = pvlib.temperature.faiman(meteo_data['Irradiance'],
                                         meteo_data['Temperature'],
                                         meteo_data['Wind'])
    return temp_cell


def _calculate_maximum_power_point(effective_irradiance, temp_cell, pv_module):
    I_L_ref, I_o_ref, R_s, R_sh_ref, a_ref, Adjust = pvlib.ivtools.sdm.fit_cec_sam(
        celltype=pv_module['celltype'],
        v_mp=pv_module['v_mp'],
        i_mp=pv_module['i_mp'],
        v_oc=pv_module['v_oc'],
        i_sc=pv_module['i_sc'],
        alpha_sc=pv_module['alpha_sc'],
        beta_voc=pv_module['beta_voc'],
        gamma_pmp=pv_module['gamma_pdc'],
        cells_in_series=pv_module['numbercells'],
        temp_ref=pv_module['temp_ref'])
    cec_parameters = pvlib.pvsystem.calcparams_cec(
        effective_irradiance=effective_irradiance,
        temp_cell=temp_cell,
        alpha_sc=pv_module['alpha_sc'],
        a_ref=a_ref,
        I_L_ref=I_L_ref,
        I_o_ref=I_o_ref,
        R_sh_ref=R_sh_ref,
        R_s=R_s,
        Adjust=Adjust)
    mpp_scenario = pvlib.pvsystem.max_power_point(*cec_parameters, method='newton')
    return mpp_scenario


def _get_solarposition(scenario_location, current_time):
    solarpos = scenario_location.get_solarposition(times=pd.date_range(start=current_time,
                                                                       end=current_time,
                                                                       tz=datetime.timezone.utc))
    return solarpos


def _request_meteodata(file_path):
    try:
        df = pd.read_csv(file_path)
        df['DateTime'] = pd.to_datetime(df['DateTime'])
        df.set_index('DateTime', inplace=True)
        return df.sort_index()
    except Exception as e:
        print(f"Error loading meteo data: {e}")
        return None


def _send_energy_to_influx_db(influx_conf, write_api, tag_id, report):
    # TODO - Write (DC and AC production) to InfluxDB, in an appropiate _mesurement, _field and ID
    # TODO - Optionally indicate the correct type for the values to the DB
    point_to_store = Point("energy_production").tag("location", tag_id).field("AC", float(report['energyACProduction'])).field("DC", float(report['energyDCProduction']))
    write_api.write(bucket=influx_conf['influx_bucket'], org=influx_conf['influx_org'], record=point_to_store)

def _send_metro_to_influx_db(influx_conf, write_api, tag_id, metro_report):
    point_to_store = Point("wether_mesurement").tag("locationj", tag_id).field("Wind", float(metro_report.get("Wind", 0.0))).field("Rain", float(metro_report.get("Rain"))).field("Temperature", float(metro_report.get("Temperature", 0.0))).field("TemperatureMax", float(metro_report.get("TemperatureMax", 0.0))).field("TemperatureMin", float(metro_report.get("TemperatureMin")))
    write_api.write(bucket=influx_conf['influx_bucket'], org=influx_conf['influx_org'], record=point_to_store)

def _get_influx_db(influx_conf):
    # TODO - Correct port
    try:
        client = InfluxDBClient(url="http://localhost:8086", token=influx_conf['influx_token'], org=influx_conf['influx_org']) #EDITED: Set correct port 8086
    except:
        client = None
    return client


def main():
    influx_conf = influx_config.influx_db_config
    UAB_config = pv_system_config.UAB_config
    pv_module = pv_system_config.pv_module
    pv_inverter = pv_system_config.pv_inverter

    influx_db_client = _get_influx_db(influx_conf)
    write_influx_api = influx_db_client.write_api(write_options=SYNCHRONOUS)
    prev_time_UAB, meteo_data, effective_irradiance_UAB, meteo_specific_UAB = None, None, None, None
    location_UAB = Location(latitude=UAB_config['latitude'],
                            longitude=UAB_config['longitude'],
                            tz=UAB_config['tz'],
                            altitude=UAB_config['altitude'],
                            name=UAB_config['name'])
    UAB_array = _get_pv_structure(scenario_configuration=UAB_config, pv_module=pv_module,
                                  modules_line=18, columns_array=18, name_array='UAB')
    system_UAB = PVSystem(arrays=UAB_array)


    
    folder_path = "WeatherData/"
    file_name = "meteo_processed.csv"

    meteo_data = _request_meteodata(os.path.join(folder_path, file_name))

    while True:
        time_now = datetime.datetime.now(datetime.timezone.utc)
        current_time = time_now.replace(year=2026) #simulate the PV system as if it were running in real-time during 2026
        # current_time = time_now.replace(year=2026, hour=12)  # force midday to see data



        meteo_specific_UAB, prev_time_UAB = _get_specific_data(meteo_data, current_time, prev_time_UAB,
                                                               meteo_specific_UAB)
        solarpos_UAB = _get_solarposition(scenario_location=location_UAB, current_time=current_time)

        if meteo_specific_UAB is not None and meteo_specific_UAB['Irradiance'] != 0: 
            effective_irradiance_UAB = _get_effective_irradiance(scenario_config=UAB_config,
                                                                 solar_position=solarpos_UAB,
                                                                 meteo_data=meteo_specific_UAB)
            temp_cell_UAB = _get_temperature_cell(meteo_data=meteo_specific_UAB)

            production_UAB = _calculate_maximum_power_point(effective_irradiance=effective_irradiance_UAB,
                                                            temp_cell=temp_cell_UAB,
                                                            pv_module=pv_module)
            dc_production_UAB = system_UAB.scale_voltage_current_power(production_UAB)
            dc_production_UAB_val = float(dc_production_UAB.iloc[0].p_mp / 1000)
            production_ac_UAB = pvlib.inverter.pvwatts(pdc=dc_production_UAB.p_mp,
                                                       pdc0=pv_inverter['pdc0'],
                                                       eta_inv_nom=pv_inverter['eta_inv_norm'],
                                                       eta_inv_ref=pv_inverter['eta_inv_ref'])
            production_ac_UAB_val = list(production_ac_UAB)[0] / 1000

        else:
            dc_production_UAB_val = 0.
            production_ac_UAB_val = 0.

        report_energy = {
            'energyACProduction': production_ac_UAB_val,
            'energyDCProduction': dc_production_UAB_val,
        }
        _send_energy_to_influx_db(influx_conf=influx_conf, write_api=write_influx_api, tag_id='UAB_Enginyeria',
                                  report=report_energy)
        
        if meteo_specific_UAB == None:
            meteo_specific_UAB = {}

        _send_metro_to_influx_db(influx_conf=influx_conf, write_api=write_influx_api, tag_id='UAB_Enginyeria',
                                  metro_report=meteo_specific_UAB)
        time.sleep(execution_period)


if __name__ == '__main__':
    main()