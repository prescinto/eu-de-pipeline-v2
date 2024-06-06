from SUD.SMA.utils import prd_utils, blob_utils
import pandas as pd

class Transform():
    def __init__(self, plant_id):
        self.plant_id = plant_id
        print(plant_id)
        
    async def main(self):
        tag_details = prd_utils.get_tag_details(self.plant_id)
        device_details = prd_utils.get_devices_from_plant(self.plant_id)
        files = await blob_utils.read_files_from_blob(self.plant_id,'Extract')
        for file_name in files:
            file_time = file_name.split('/')[-1].split('_')[1]
            print(f"{file_time=}")
            transform_json = await self.transform_data(file_name, tag_details, device_details)
            await blob_utils.save_data_to_blob_storage(self.plant_id, transform_json, file_time, 'Transform')
        await self.mark_files_processed(files)
    
    async def transform_data(self, file_name, tag_details, device_details):
        json_data = blob_utils.read_data_file_blob(file_name)
        
        transform_json = dict()
        for device_cat in json_data.keys():
            try:
                final_data =dict()
                final_data[device_cat] = dict()
                df = pd.DataFrame(json_data[device_cat])
                device_mapping = {}
                for d in device_details:
                    device_mapping[d[2]]=d[3]
                df['device_id'] = df['device_id'].map(device_mapping)
                df.set_index('time', inplace=True)
                # tag_details
                tag_names = self.get_tag_index(tag_details, device_cat)
                devices = list(df["device_id"].unique())
                for sub_device in devices:
                    print(sub_device)
                    dataframe = df[df['device_id']==sub_device]
                    dataframe = pd.concat([pd.DataFrame(columns=tag_names), dataframe])
                    dataframe = dataframe[tag_names]
                    dataframe['time'] = pd.to_datetime(dataframe.index).strftime("%Y-%m-%d %H:%M:%S")
                    dataframe[device_cat] = sub_device
                    final_data[device_cat][sub_device] = dataframe.to_dict(orient='records')
                transform_json[device_cat] = final_data
            except Exception as e:
                print(f"Error: {e}")
        return transform_json

        
    
    async def mark_files_processed(self, files):
        file_names = [x.split('/')[-1] for x in files]
        print(f"Transform {file_names=}")
        await blob_utils.move_blob(self.plant_id, file_names, 'Extract')
        
    
    def get_tag_index(self, tag_details, device_cat):
        tag_df = pd.DataFrame(tag_details)
        tag_df = tag_df[tag_df[2]==device_cat]
        req_index_df = pd.DataFrame([i for i in range(1,tag_df[5].max()+1)])
        tag_df = pd.merge(tag_df, req_index_df, how='outer', left_on=5, right_on=0, validate="many_to_many")
        tag_df.fillna('',inplace=True)
        tag_names = tag_df.sort_values(5)[3].tolist()
        return tag_names