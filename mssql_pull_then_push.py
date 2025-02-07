import os 

# https://pypi.org/project/pymssql/
import pymssql
import json

from datetime import date, datetime

# https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-install
import snowflake.connector

object_types = ["columns", "default_constraints", "foreign_keys", "foreign_key_columns", 
                "identity_columns", "indexes", "index_columns", "key_constraints", "objects", 
                "schemas", "tables", "types", "views"]

# during json encoding dates need to be formated as strings
def json_pre_encoder(obj):

    if isinstance(obj, (datetime, date)):
        return obj.isoformat(timespec='seconds')
    else:

        if hasattr(obj, '__dict__'):
            return obj.__dict__

# this class serves as an interface between the local libaries
# and your sql instance 

class mssql_conn():

    def __init__(self, config, target_database) -> None:
        
        self.config = config

        self.target_database = target_database

        self.connection = None
        self.cursor = None
        self.sqlquery = None
        self.result_columns = None
        self.result_column_names = None
        self.rows = []

        self.connect()

    def connect(self) -> None:

        if self.config["windowsauth"]:
            self.connection = pymssql.connect(server=self.config["host"], port=self.config["port"], database=self.target_database)
        else:
            self.connection = pymssql.connect(server=self.config["host"], port=self.config["port"], user=self.config["username"], password = self.config["password"], database=self.target_database)

        self.cursor = self.connection.cursor()

    def close(self) -> None:

        if self.cursor != None:
            self.cursor.close()

    def execute(self, sqlquery:str) -> None:

        if self.cursor != None and sqlquery != "":

            self.sqlquery = sqlquery
            self.cursor.execute(sqlquery)

            self.result_columns = mssql_result_columns(self.cursor.description)
            self.result_column_names = [col[0] for col in self.cursor.description]

    def fetch_one(self):

        self.rows = []
        self.rows.append(self.cursor.fetchone())

        return self.rows

    def fetch_many(self, batch_size:int=50000):

        self.rows.append(self.cursor.fetchmany(size=batch_size))

        return self.rows

    def fetch_to_file(self, object_type:str, file_location:str="", batch_size:int=50000):

        last_batch = 0

        if self.result_columns != None:

            self.rows = []

            filename = os.path.join(file_location, object_type + "_metadata.json")
            self.result_columns.save(filename)

            filename = os.path.join(file_location, object_type + "_rows.json")
            with open(file=filename, mode="w") as f:

                while True:
                    self.fetch_many(batch_size=batch_size)

                    last_batch = len(self.rows) - 1

                    if not self.rows[last_batch]:
                        self.rows.pop(last_batch)
                        break

                    for row in self.rows[last_batch]:

                        dict_row = dict(zip(self.result_column_names, row))

                        json.dump(dict_row, f, default=json_pre_encoder)
                        f.write("\n")

class mssql_result_column():

    def __init__(self, sequence, column):

        self.sequence = sequence
        self.name = column[0]
        self.type_code = column[1]
        self.type = self.convert_type_code(column[1])
        self.display_size = column[2]
        self.internal_size = column[3]
        self.precision = column[4]
        self.scale = column[5]
        self.null_ok = column[6]

    def convert_type_code(self, type_code):

        match type_code:

            case 1:
                return "STRING"

            case 2:
                return "NUMBER"

            case 3:
                return "NUMBER"

            case 4:
                return "DATETIME"

            case 5:
                return "ROWID"


class mssql_result_columns():

    def __init__(self, descriptions):

        self.list = []

        sequence = 0

        for elem in descriptions:

            column = mssql_result_column(sequence, elem)

            self.list.append(column)

            sequence = sequence + 1

    def save(self, file_name:str):

        with open(file=file_name, mode="w") as f:

            for column in self.list:
                f.write(json.dumps(column.__dict__) + "\n")

def mssql_pull_schema(root_data_path:str, config:dict, target_database:str):

    file_location = os.path.join(root_data_path, target_database)
    os.makedirs(name=file_location, exist_ok=True)

    conn = mssql_conn(config, target_database)

    for object_type in object_types:

        if object_type == "identity_columns":

            conn.execute("""SELECT object_id,name,column_id,system_type_id,user_type_id,max_length,precision,scale, 
                        collation_name,is_nullable,is_ansi_padded,is_rowguidcol,is_identity,is_filestream,is_replicated, 
                        is_non_sql_subscribed,is_merge_published,is_dts_replicated,is_xml_document,xml_collection_id, 
                        default_object_id,rule_object_id,CONVERT(int,seed_value) seed_value,CONVERT(int,increment_value) increment_value,CONVERT(int,last_value) last_value,
                        is_not_for_replication,is_computed,is_sparse,is_column_set,generated_always_type,
                        generated_always_type_desc,encryption_type,encryption_type_desc,encryption_algorithm_name,
                        column_encryption_key_id,column_encryption_key_database_name,is_hidden,is_masked,graph_type,
                        graph_type_desc 
                        FROM sys.identity_columns""")

        else:
            conn.execute("SELECT * FROM sys." + object_type)

        conn.fetch_to_file(object_type=object_type, file_location=file_location)

        if object_type == "views":

            metadata = '{"sequence": 0, "name": "name", "type_code": 1, "type": "STRING", "display_size": null, "internal_size": null, "precision": null, "scale": null, "null_ok": null}\n' + \
                       '{"sequence": 1, "name": "object_id", "type_code": 3, "type": "NUMBER", "display_size": null, "internal_size": null, "precision": null, "scale": null, "null_ok": null}\n' + \
                       '{"sequence": 2, "name": "definition", "type_code": 1, "type": "STRING", "display_size": null, "internal_size": null, "precision": null, "scale": null, "null_ok": null}'

            filename = file_location + "\\view_definitions_metadata.json"

            with open(file=filename, mode="w") as f:
                f.write(metadata + "\n")

            filename = file_location + "\\view_definitions_rows.json"

            with open(file=filename, mode="w") as f:
                
                for batch in conn.rows:

                    for view in batch:

                        conn.execute(f"SELECT definition FROM sys.sql_modules WHERE object_id = {view[1]};")
                        row = conn.fetch_one()

                        if len(row) == 1:
                            view_definition = row[0][0]

                            if view_definition != None:

                                view_definition = view_definition.replace(r'"',r'\"')
                                view_definition = view_definition.replace('\r\n',r'\n')

                                f.write(f"{{\"name\": \"{view[0]}\", \"object_id\": {view[1]}, \"definition\": \"{view_definition}\" }}\n")
           

    conn.close()
 

def push_to_snowflake_file(object_type:str, file_location:str, target_database, cursor):

    filename = file_location + "\\" + target_database + "\\" + object_type + "_rows.json"

    if os.path.exists(filename):

        # if the file exists on the snowflake stage then remove it (just making sure)
        cursor.execute(f"REMOVE @TO_APP/MSSQL/{filename}")

        # copy the file from the local directory to the snowflake stage
        cursor.execute(f"PUT file://{filename} @TO_APP/MSSQL AUTO_COMPRESS=FALSE OVERWRITE=TRUE")


def push_to_snowflake(root_data_path:str, config:dict, target_database:str):

    # connect to snowflake instance

    try:

        conn = snowflake.connector.connect( user=config["username"], 
                                            password=config["password"], 
                                            account=config["account"], 
                                            role=config["role"], 
                                            warehouse=config["warehouse"],
                                            database=config["appdatabase"],
                                            schema="_METADATA" )

        cursor = conn.cursor()        
        
        current_database = cursor.execute("SELECT CURRENT_DATABASE()").fetchone()

        if current_database[0] == cursor.connection.database:

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

        else:
            print("")
            print(f'Snowflake Database/App {config["appdatabase"]} Not Found or Not Authorized for user {config["username"]}')

    except Exception as e:

        print("")
        match e.errno:

            case 250003:
                print(f'Snowflake Account {config["account"]} Not Found')

        print(e)


if __name__ == "__main__":


    #####                          PERMISSIONS REQUIRED                          #####
    
    #####  In the database that is the target for migration the user connecting  #####
    #####  needs a minimum set of permissions.   This client application does    #####
    #####  not select any data from your tables only meta data from the system   #####
    #####  views.                                                                #####
        
    #####  You can choose to use a current login or create a new one.  In this   #####
    #####  Example we will create a new use with minimal rights.                 #####
    #####  Example:    CREATE USER migratetest;                                  #####

    #####  In order to read tables, columns, ...  You need db_datareader         #####
    #####  Example:    ALTER ROLE db_datareader ADD MEMBER migratetest;          #####

    #####  In order to read views, ...  You need    VIEW DEFINITION              #####
    #####  Example:    GRANT VIEW DEFINITION TO migratetest;                     #####


    # target database for this pull
    target_database = "AdventureWorks2019"

    mssql_config = {}
    mssql_config["host"] = "<<SQL SERVER HOST NAME OR IP>>"
    mssql_config["port"] = "1433"

    # if connecting via Windows Auth
    #mssql_config["username"] = ""         # not used during windows authentication
    #mssql_config["password"] = ""         # not used during windows authentication
    #mssql_config["windowsauth"] = True

    # if connecting via sql credentials
    mssql_config["username"] = "<<SQL USER NAME>>"
    mssql_config["password"] = "<<SQL PASSWORD>>"
    mssql_config["windowsauth"] = False


    snowflake_config = {}
    snowflake_config["account"]     = "<<REPLACE WITH YOUR SNOWFLAKE ACCOUNT XXXXXX-YYYYYYY>>"
    snowflake_config["username"]    = "<<REPLACE WITH YOUR SNOWFLAKE USERNAME>>"
    snowflake_config["password"]    = "<<REPLACE WITH YOUR SNOWFLAKE PASSWORD>>"
    snowflake_config["role"]        = "ACCOUNTADMIN"
    snowflake_config["warehouse"]   = "<<REPLACE WITH YOUR COMPUTE NAME>>"
    snowflake_config["appdatabase"] = "<<THIS IS THE NAME OF THE SNOWFLAKE APPLICATION>>"



    # assign a storage area for the temporary landing of the data
    root_data_path = "C:\\Temp\\mssql\\"

    # need to make sure entire root data path is available
    os.makedirs(name=root_data_path, exist_ok=True)

    # pull metadata from your local database
    mssql_pull_schema(root_data_path=root_data_path, config=mssql_config, target_database=target_database)

    # push metadata to snowflake
    push_to_snowflake(root_data_path=root_data_path, config=snowflake_config, target_database=target_database)
    