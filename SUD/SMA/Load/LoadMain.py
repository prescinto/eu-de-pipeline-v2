from SUD.SMA.utils import prd_utils, blob_utils
import numpy as np
import os
import time
import gzip
from datetime import datetime
from ftplib import FTP
import pysftp
import gzip
import shutil
import logging


class Load():
    def __init__(self, plant_id):
        self.plant_id = plant_id
        print(plant_id)
        
    async def main(self):
        ftp_details = prd_utils.get_ftp_details(self.plant_id)
        transform_json_files = await blob_utils.read_files_from_blob(self.plant_id,'Transform')
        
        for json_file in transform_json_files:
            await self.create_upload_csv(json_file, ftp_details)
            
        await self.mark_files_processed(transform_json_files)
        return
    
    async def create_upload_csv(self, file_name, ftp_details):
        file_time = file_name.split('/')[-1].split('_')[1]
        print(f"{file_time=}")
        json_data = blob_utils.read_data_file_blob(file_name)
        ftp_host = ftp_details[0]
        ftp_user_id = ftp_details[1]
        ftp_password = ftp_details[2]
        ftp_port = ftp_details[3]
        ftp_is_sftp = ftp_details[4]
        FtpUpload(self.plant_id, json_data, ftp_host, ftp_user_id, ftp_password, ftp_port, ftp_is_sftp, file_time).process()
        return
    
    
    async def mark_files_processed(self, files):
        file_names = [x.split('/')[-1] for x in files]
        await blob_utils.move_blob(self.plant_id, file_names, 'Transform')
        return
    

class FtpUpload():

    def __init__(self, plant_id, json_data, ftp_host, ftp_user_id, ftp_password, ftp_port, ftp_is_sftp=True, file_time=''):
        self.Host = ftp_host
        self.UserId = ftp_user_id
        self.Password = ftp_password
        self.Port = ftp_port
        self.IsFtp = (ftp_is_sftp==False)

        # DE Logger
        self.PlantId = plant_id
        self.EventSource = 'Load'
        self.deLogger = logging.getLogger()
        self.DestinationDirectory = plant_id

        # sd = config['SUBDIRETORIES']
        self.sub_directory_key = dict()
        # for key in list(sd.keys()):
        #     self.sub_directory_key[key] = sd[key]
        self.json_data = json_data
        self.file_time = file_time
    
    def check_if_any_devices_has_data(self,list_devices,row_data_new):
        data_written = any(dev in row_data_new and len(row_data_new[dev]) > 0 for dev in list_devices)
        return data_written
        
        
    def create_csv_for_device(self, device_key, device_name, row_data_new):
        print(f"create_csv_for_device: {device_name=}")
        try:
            dev_col = device_name
            file_name = f"/home/prescintouser/SUD/SMA/{device_name}_{self.file_time}_{str(int(time.time()))}.csv"
            print(f"create_csv_for_device: {file_name=}")
            # file_name = f"{device_name}_{str(int(time.time()))}.csv"
            time_col = 'time'
            if file_name is None or file_name=='None':
                print('Hello')
            list_devices = list(row_data_new.keys())
            
            if self.check_if_any_devices_has_data(list_devices,row_data_new):
                with open(file_name, 'w+') as fp:
                    for dev in list_devices:
                        try:
                            dev_data = row_data_new[dev]
                            if len(dev_data)>0:
                                fp.write(f'deviceType,{dev}\n')
                                columns = [key for key in list(dev_data[0].keys()) if key not in [dev_col, time_col]]
                                col_to_write = [f'"{key}"' for key in list(dev_data[0].keys()) if key not in [dev_col, time_col]]
                                fp.write(f',{dev},{",".join(col_to_write)}\n')
                                columns = [time_col, dev_col] + columns
                                for d in dev_data:
                                    fp.write(f'{",".join([str(d[col]) for col in columns])}\n')
                        except Exception as e:
                            self.deLogger.warning(plant_id=self.PlantId, event_id=np.nan,
                                    event_message=f'Data not find for device" {dev} --->{dev_data}', event_time=str(datetime.now()),
                                    event_source=self.EventSource)
            else:
                self.deLogger.warning(plant_id=self.PlantId, event_id=np.nan,
                                    event_message=f'Data file is empty!', event_time=str(datetime.now()),
                                    event_source=self.EventSource)
            print(f"After Writing: {file_name=}")
            return file_name
        except Exception as e:
            # self.deLogger.warning(plant_id=self.PlantId, event_id=np.nan,
            #                       event_message=f'Exception in creating csv: ' + str(e), event_time=str(datetime.now()),
            #                       event_source=self.EventSource)
            print(f'Exception in creating csv: ' + str(e))
            return ''
        
    
    def compress_to_gzip(self, input_file):
        outfile_name = f'{input_file}.gz'
        with open(input_file, 'rb') as f_in:
            with gzip.open(outfile_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(input_file)
        return outfile_name

    def upload_to_ftp(self, ftp, input_file, device_key):
        if self.sub_directory_key.get(device_key.lower()) is None:
            return
        ftp.cwd(self.DestinationDirectory)
        subDirectory = self.sub_directory_key.get(device_key.lower())
        try:
            ftp.mkd(subDirectory)
        except:
            pass
        ftp.cwd(subDirectory)
        with open(input_file, 'rb') as fp:
            ftp.storbinary(f"STOR {input_file}", fp)
        self.deLogger.info(plant_id=self.PlantId, event_id=np.nan, event_message=f"Uploaded file - {input_file}",
                           event_time=str(datetime.now()), event_source=self.EventSource)
        os.remove(input_file)
        ftp.cwd("...")
        self.deLogger.info(plant_id=self.PlantId, event_id=np.nan,
                           event_message=f'Local file removed and FTP connection closed.', event_time=str(datetime.now()),
                           event_source=self.EventSource)

    def upload_to_sftp(self, sftp, input_file, device_key):
        if device_key is None:
            return
        try:
            sftp.mkdir(self.DestinationDirectory)
        except:
            pass
        
        with sftp.cd(self.DestinationDirectory):
            subDirectory = device_key#self.sub_directory_key.get(device_key.lower())
            try:
                sftp.mkdir(subDirectory)
            except:
                pass
            with sftp.cd(subDirectory):
                sftp.put(input_file)
                self.deLogger.info(plant_id=self.PlantId, event_id=np.nan,
                                   event_message=f"Uploaded file - {input_file}", event_time=str(datetime.now()),
                                   event_source=self.EventSource)
        os.remove(input_file)
        self.deLogger.info(plant_id=self.PlantId, event_id=np.nan,
                           event_message=f'Local file removed and FTP connection closed.', event_time=str(datetime.now()),
                           event_source=self.EventSource)


    def save_data_ftp(self):
        with FTP(host=self.Host) as ftp:
            ftp.login(self.UserId, self.Password)
            for device_key in self.sub_directory_key.keys():
                for device_name in self.json_data[device_key].keys():
                    try:
                        row_data = self.json_data[device_key][device_name]
                        file_name = self.create_csv_for_device(device_key, device_name, row_data)
                        gzip_file = self.compress_to_gzip(file_name)
                        self.deLogger.info(plant_id=self.PlantId, event_id=np.nan,
                                           event_message=f'Gzip Created - {gzip_file}. Uploading to FTP...',
                                           event_time=str(datetime.now()), event_source=self.EventSource)
                        try:
                            self.upload_to_ftp(ftp, gzip_file, device_key)
                        except:
                            ftp = FTP(host=self.Host)
                            ftp.login(self.UserId, self.Password)
                            self.upload_to_ftp(ftp, gzip_file, device_key)
                        print(f'Upload to FTP Complete for {gzip_file}.')
                    except Exception as ex:
                        self.deLogger.error(plant_id=self.PlantId, event_id=np.nan,
                                            event_message=f'Failure in Uploading Files - {str(ex)}',
                                            event_time=str(datetime.now()), event_source=self.EventSource)

    def save_data_sftp(self):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(self.Host, username=self.UserId, password=self.Password, cnopts=cnopts) as sftp:
            print("FTP Connected...")
            for device_key, device_data in self.json_data.items():
                # for device_name in self.json_data[device_key].keys():
                for device_name,data in device_data.items():
                    try:
                        row_data = data#self.json_data[device_key][device_name]
                        row_data_new = row_data
                        file_name = self.create_csv_for_device(device_key, device_name, row_data_new)
                        gzip_file = self.compress_to_gzip(file_name)
                        print(f'{gzip=}')
                        # self.deLogger.info(plant_id=self.PlantId, event_id=np.nan,
                        #                    event_message=f'Gzip Created - {gzip_file}. Uploading to FTP...',
                        #                    event_time=str(datetime.now()), event_source=self.EventSource)
                        print(f'Gzip Created - {gzip_file}. Uploading to FTP...')
                        try:
                            self.upload_to_sftp(sftp, gzip_file, device_name)
                            print(f"File {file_name}, {gzip_file}")
                        except:
                            sftp = pysftp.Connection(self.Host, username=self.UserId, password=self.Password,
                                                     cnopts=cnopts)
                            self.upload_to_sftp(sftp, gzip_file, device_name)
                        print(f'Upload to FTP Complete for {gzip_file}.')
                    except Exception as ex:
                        # self.deLogger.error(plant_id=self.PlantId, event_id=np.nan,
                        #                     event_message=f'Failure in Uploading Files - {str(ex)} {gzip_file}',
                        #                     event_time=str(datetime.now()), event_source=self.EventSource)
                        print(f'Failure in Uploading Files - {str(ex)} {gzip_file}')
        return
    
    def process(self):
        if self.IsFtp:
            self.save_data_ftp()
        else:
            self.save_data_sftp()