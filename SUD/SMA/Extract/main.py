import configparser

import numpy as np
import pytz
import requests
import json
import pandas as pd
import os
import ast
import logging
from datetime import datetime
from SUD.SMA.utils import prd_utils
from SUD.SMA.utils import blob_utils
import asyncio



class Extract():
    def __init__(self, plant_id):
        fname = __file__
        path_to_config = "/".join(fname.split("/")[:-1])
        config_file_path = os.path.join(path_to_config, "config.ini")
        config = configparser.ConfigParser()
        config.read(config_file_path)
        self.plant_id = plant_id
        self.config = config
        self.BaseUrl = config['Authentication']['BaseUrl']
        self.BaseUrlToken = config['Authentication']['BaseUrl_token']
        self.ClientSecret = config['Authentication']['client_secret']
        self.ClientId = config['Authentication']['client_id']
        self.PipelineId = config['Authentication']['pipeline_id']
        self.PlanntTZ = config['Authentication']['plant_time_zone'] 
        # self.PrescintoPlantIds = config['Authentication']['prescintoplantIds'].split(",")
        self.EventSource = config['Logger']['EventSource']
        # self.deLogger = DeLogger(app_name=config['Logger']['AppName'], app_id=int(config['Logger']['AppId']))
        self.deLogger = logging.getLogger()
        # self.plant_time_zone = config['Authentication']['plant_time_zone']

    def login_interface(self):
        """
        For login API
        :return: Token
        """
        token = None
        try:
            url = f'{self.BaseUrlToken}/token'
            request_body = {'grant_type': 'client_credentials', 
                            'client_secret': self.ClientSecret,
                            'client_id': self.ClientId}
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            res = requests.post(url=url, data=request_body, headers=headers, timeout=10)
            token = ast.literal_eval(res.text)['access_token']
        except Exception as e:
            self.deLogger.critical(plant_id=int(self.PlantIds[0]), event_id=np.nan, event_message=f'{str(e)}',
                                   event_time=datetime.now(),
                                   event_source=self.EventSource)
        return token

    def get_measurements_of_device(self, device_id, token):
        measurements = []
        try:
            url = f'{self.BaseUrl}/v1/devices/{device_id}/measurements/sets'
            request_body = {"grant_type": 'client_credentials', 
                            'client_secret': self.ClientSecret,
                            'client_id': self.ClientId}
            headers = {'Content-Type': 'application/x-www-form-urlencoded', 'Authorization': f'Bearer {token}'}
            res = requests.get(url=url, data=request_body, headers=headers, timeout=10)
            json_data = json.loads(res.text)
            measurements = json_data['sets']
        except Exception as e:
            print(e)
        return measurements

    def generate_flat_data1(self, final_items, param, child_name):
        measurement_name = child_name.split('/')[0]
        final_items[f'{measurement_name}' + '_' + param['index']] = param['value']
        return final_items

    def generate_flat_data(self, final_items, item, parent_name):
        """
        Convert JSON data to flat data

        :param final_items: Dictionary
        :param item: Items in json files
        :param parent_name:
        :return: Flat data
        """
        if type(item) is list:
            for i in range(len(item)):
                child_name = str(i) if parent_name == "" else parent_name + '/' + str(i)
                final_items = self.generate_flat_data1(final_items, item[i], child_name)
        elif type(item) is str:
            final_items[parent_name] = item
        elif type(item) is int:
            final_items[parent_name] = item
        elif type(item) is dict:
            for key, value in item.items():
                child_name = key if parent_name == "" else parent_name + '/' + key
                final_items = self.generate_flat_data(final_items, value, child_name)
        else:
            final_items[parent_name] = item
        return final_items

    def get_df(self, data):
        """
            Convert JSON data to Pandas data frame
            :param data: Data in JSON format
            :return: Dataframe
            """

        data_info = []
        for item in data:
            final_items = {}
            final_items = self.generate_flat_data(final_items, item, parent_name="")
            data_info.append(final_items)
        df = pd.DataFrame(data_info)
        return df


    def get_live_data(self, token, device_id, measurement, date):
        df = pd.DataFrame()
        try:
            url = f'{self.BaseUrl}/v1/devices/{device_id}/measurements/sets/{measurement}/Day?Date={date}'
            headers = {'Authorization': f'Bearer {token}'}
            response = requests.get(url=url, headers=headers, timeout=10)
            if response.status_code == 200:
                json_data = json.loads(response.text)
                set_data = json_data['set']
                df = self.get_df(set_data)
            if df.empty:
                self.deLogger.critical(plant_id=self.plant_id, event_id=np.nan, event_message=f'{str(e)}',
                        event_time=datetime.now(),
                        event_source=self.EventSource)
        except Exception as e:
            print(e)
        return df

    def get_device_data(self, device_id, token, date):
        measurements = self.get_measurements_of_device(device_id, token)
        main_device_df = pd.DataFrame()
        if len(measurements) > 0:
            for measurement in measurements:
                try:
                    raw_df = self.get_live_data(token, device_id, measurement, date)
                    if not raw_df.empty:
                        raw_df['time'] = pd.to_datetime(raw_df['time'], format='%Y-%m-%dT%H:%M:%S')
                        if main_device_df.empty:
                            main_device_df = raw_df
                        else:
                            main_device_df = pd.merge(main_device_df, raw_df, how='left', on='time')
                except Exception as e:
                    self.deLogger.critical(plant_id=int(self.PlantIds[0]), event_id=np.nan, 
                                           event_message=f'{str(e)}',event_time=datetime.now(),
                                           event_source=self.EventSource)
        return main_device_df

    def get_data(self):
        devices = prd_utils.get_devices_from_plant(self.plant_id)
        apiToken = self.login_interface()
        date = datetime.now(tz=pytz.timezone(self.PlanntTZ)).date().strftime('%Y-%m-%d')
        deviceTypes = list(set([i[4] for i in devices]))
        json_data = dict()
        deviceTypes = list(set([i[4] for i in devices]))
        for deviceTyp in deviceTypes:
            devicesT = [i for i in devices if i[4]==deviceTyp]
            main_device_df = pd.DataFrame()
            for device in devicesT:
                device_id = device[2] #source_device_id from DB
                device_df = self.get_device_data(device_id, apiToken, date)
                device_df['device_id'] = device_id
                device_df['device_type'] = deviceTyp
                main_device_df = pd.concat([main_device_df, device_df])
            if not main_device_df.empty:
                dtColumns = main_device_df.dtypes[pd.Series(main_device_df.dtypes)=="datetime64[ns]"].index.tolist()
                for i in dtColumns:
                    main_device_df[i] = main_device_df[i].dt.strftime("%Y-%m-%d %H:%M:%S")
            json_data[deviceTyp] = main_device_df.to_dict('records')
        file_time = datetime.now().strftime("%Y%m%d%H%M%S")
        print(f"{file_time=}")
        asyncio.run(blob_utils.save_data_to_blob_storage(self.plant_id, json_data, file_time, 'Extract'))
        return 
