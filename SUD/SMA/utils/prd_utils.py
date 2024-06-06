import psycopg2
import pandas as pd


"""
This function connects to a PostgreSQL database using the psycopg2 library.

Returns:
    psycopg2.extensions.connection: The connection object if the connection is successful.

Raises:
    Exception: If there is an error in connecting to the PostgreSQL database.
"""
def connect_postgresql():
    try:
        conn = psycopg2.connect( 
                database="longclaw", user='presdbuser',  
                password='Ba3ahm0pU8$', host='172.16.13.4', port='5432'
        ) 
        print('DB connection success...')
        return conn
    except Exception as e:
        print(f'Error in connecting POSTGRESQL: {str(e)}')

"""
Function: get_prds

Parameters:
- pipeline_id: (int) The ID of the pipeline

Returns:
- prds: (list) A list of records from the prd_plants table that are associated with the given pipeline_id and are active.
"""
def get_prds(pipeline_id):
    with connect_postgresql() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            Select * from prd_plants where prd_id IN(
            Select prd_id from PRD_Pipelines where pipeline_id=%s and is_active=True
            ) and is_active=True
            """, (pipeline_id,))
            prds = cursor.fetchall()
    return prds


def get_devices_from_plant(plant_id):
    with connect_postgresql() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            Select * from prd_devices where prd_id IN (
                Select prd_id from PRD_plants where plant_id=%s and Is_Active=True
            ) and Is_Active=True;
            """, (plant_id,))
            devices = cursor.fetchall()
    return devices

"""
Function: get_prd_devices

Parameters:
- pipeline_id: The ID of the pipeline to retrieve PRD devices for.

Returns:
- prd_devices: A list of PRD devices that are active and associated with the given pipeline ID.

Description:
This function queries the PostgreSQL database to retrieve PRD devices based on the provided pipeline ID. It first connects to the database using the connect_postgresql() function. Then, it executes a SQL query to select all PRD devices from the 'prd_devices' table where the PRD ID is in the result of a subquery. The subquery selects the PRD ID from the 'PRD_Pipelines' table where the pipeline ID matches the provided pipeline_id and the 'is_active' column is set to True. Finally, the function returns the fetched PRD devices as a list.

Note: The 'is_active' column is used to filter out inactive PRD devices from the result.
"""
def get_prd_devices(pipeline_id):
    with connect_postgresql() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            Select * from prd_devices where prd_id IN(
            Select prd_id from PRD_Pipelines where pipeline_id=%s and is_active=True
            ) and is_active=True
            """, (pipeline_id,))
            prd_devices = cursor.fetchall()
    return prd_devices




"""
Function: get_prd_tags

Parameters:
- pipeline_id: The ID of the pipeline to retrieve PRD tags for

Returns:
- prd_tags: A list of PRD tags associated with the given pipeline ID

Description:
This function retrieves PRD tags from the PostgreSQL database based on the provided pipeline ID. It connects to the database, executes a SQL query to select PRD tags that are associated with an active PRD pipeline, and returns the results as a list of tuples.
"""
def get_prd_tags(pipeline_id):
    with connect_postgresql() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            Select * from prd_tags where prd_id IN(
            Select prd_id from PRD_Pipelines where pipeline_id=%s and is_active=True
            ) and is_active=True
            """, (pipeline_id,))
            prd_tags = cursor.fetchall()
    return prd_tags


def get_tag_details(plant_id):
    with connect_postgresql() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            Select * from PRD_Tags where prd_id IN(
            Select prd_id from PRD_Plants where Plant_Id=%s and is_active=True
            ) and is_active=True
            """, (plant_id,))
            prd_tags = cursor.fetchall()
    return prd_tags


def get_ftp_details(plant_id):
    with connect_postgresql() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            Select ftp_host, ftp_user_id, ftp_password, ftp_port, is_sftp from PRD_Ftp_Details where prd_id IN(
                Select prd_id from PRD_Plants where Plant_Id=%s and is_active=True
            ) and is_active=True
            """, (plant_id,))
            prd_ftp_details = cursor.fetchone()
    return prd_ftp_details