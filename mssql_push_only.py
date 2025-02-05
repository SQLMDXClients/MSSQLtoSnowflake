import os 

# https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-install
import snowflake.connector

object_types = ["columns", "default_constraints", "foreign_keys", "foreign_key_columns", 
                "identity_columns", "indexes", "index_columns", "key_constraints", "objects", 
                "schemas", "tables", "types", "views"]

def push_to_snowflake_file(object_type:str, file_location:str, target_database, cursor):

    filename = file_location + "\\" + target_database + "\\" + object_type + "_rows.json"

    if os.path.exists(filename):

        # if the file exists on the snowflake stage then remove it (just making sure)
        cursor.execute(f"REMOVE @TO_APP/MSSQL/{filename}")

        # copy the file from the local directory to the snowflake stage
        cursor.execute(f"PUT file://{filename} @TO_APP/MSSQL AUTO_COMPRESS=FALSE OVERWRITE=TRUE")


def push_to_snowflake(root_data_path:str, config:dict, target_database:str):

    # connect to snowflake instance
    conn = snowflake.connector.connect(user=config["username"], 
                                       password=config["password"], 
                                       account=config["account"], 
                                       role=config["role"], 
                                       warehouse=config["warehouse"],
                                       database=config["appdatabase"],
                                       schema="_METADATA")

    cursor = conn.cursor()        

    for object_type in object_types:

        # push the file to snowflake

        push_to_snowflake_file(object_type=object_type, 
                               file_location=root_data_path, 
                               target_database=target_database, 
                               cursor=cursor)

    # view definitions doesn't come directly from a mssql view but 
    # is pulled individually

    push_to_snowflake_file(object_type="view_definitions", 
                            file_location=root_data_path, 
                            target_database=target_database, 
                            cursor=cursor)


if __name__ == "__main__":

    # in this example we are pulling the AdventureWorks2019 database
    # replace it with the name of your target database

    target_database = "AdventureWorks2019"

    snowflake_config = {}
    snowflake_config["account"]     = "<<REPLACE WITH YOUR SNOWFLAKE ACCOUNT XXXXXX-YYYYYYY>>"
    snowflake_config["username"]    = "<<REPLACE WITH YOUR SNOWFLAKE USERNAME>>"
    snowflake_config["password"]    = "<<REPLACE WITH YOUR SNOWFLAKE PASSWORD>>"
    snowflake_config["role"]        = "ACCOUNTADMIN"
    snowflake_config["warehouse"]   = "<<REPLACE WITH YOUR COMPUTE NAME>>"
    snowflake_config["appdatabase"] = "<<THIS IS THE NAME OF THE SNOWFLAKE APPLICATION>>"

    # assign a storage area for the temporary landing of the data
    # replace with a local subdirectory on your computer
    root_data_path = "C:\\Temp\\mssql\\"

    # push metadata to snowflake
    push_to_snowflake(root_data_path=root_data_path, config=snowflake_config, target_database=target_database)
    