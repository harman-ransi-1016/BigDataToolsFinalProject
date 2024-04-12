import yaml
import redis
import pandas as pd
import json
import logging
import seaborn as sns
import matplotlib.pyplot as plt

class RedisDB:
    """
    A class for uploading data to a Redis database.

    Attributes:
    - config_file (str): The path to the YAML configuration file containing Redis configurations.
    - redis_config (dict): A dictionary containing Redis configuration parameters.
    - redis_client (redis.Redis): The Redis client used for interacting with the Redis database.
    """

    # Constructor... creates Redis db
    def __init__(self, config_file='config.yaml'):
        """
        Initializes the RedisUploader object.

        Args:
        - config_file (str): The path to the YAML configuration file containing Redis configurations.
        """
        self.mysql_config = None
        self.redis_config = None
        self.load_config(config_file)
        self.redis_client = redis.Redis(
            host=self.redis_config['host'],
            port=self.redis_config['port'],
            db=self.redis_config['db'],
            password=self.redis_config.get('password', None) 
        )

    # Constructor Helper... access yaml file
    def load_config(self, config_file):
        """
        Loads Redis configurations from the specified YAML configuration file.

        Args:
        - config_file (str): The path to the YAML configuration file.
        """
        with open(config_file, 'r') as file:
            config_data = yaml.safe_load(file)
            self.redis_config = config_data['redis']

    # Function 1... upload csv data to Redis (key-value)
    def upload_csv_to_redis(self, csv_path):
        """ 
        Uploads data from a CSV file to Redis.

        Args:
        - csv_path (str): The path to the CSV file to be uploaded to Redis.

        Returns:
        - bool: True if the upload is successful, False otherwise.
        """
        self.redis_client.flushall()
        try:
            # 1a) read in data, convert to json
            df = pd.read_csv(csv_path)
            data = df.to_dict(orient='records')
            total_records = len(data)

            # 1b) upload json to redis (make key-val)
            for idx, record in enumerate(data):
                record_json = json.dumps(record)
                self.redis_client.set(f'record_{idx}', record_json)
                print(f"\rUploading record {idx + 1}/{total_records}", end='')

            print("\nUpload complete.")
            return True
        
        # 1c) error
        except Exception as e:
            print(f"\nError uploading data from CSV to Redis: {e}")
            return False
    
    # Function 2... download Redis data to df & return df
    def redis_data_to_dataframe(self):
        """
        Retrieves data from Redis and converts it into a pandas DataFrame.

        Returns:
        - df (pd.DataFrame or None): A pandas DataFrame containing the data retrieved from Redis,
                                    or None if an error occurs.
        """
        try:
            # 2a) fetch keys of data
            data = []
            total_keys = len(self.redis_client.keys())

            # iterate thru ndx & keys, fetch value
            for idx, key in enumerate(self.redis_client.keys()):
                redis_data = self.redis_client.get(key)
                if redis_data is not None:
                    redis_data_str = redis_data.decode('utf-8')
                    data_dict = json.loads(redis_data_str)
                    data.append(data_dict)

            # 2b) save data if we see anything
            if data:
                df = pd.DataFrame(data)
                return df
            else:
                print("No data found in Redis.")
                return pd.DataFrame() 
                    
        # 2c) error handling
        except redis.exceptions.RedisError as e:
            logging.error(f"Error connecting to Redis: {e}")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding JSON: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            return None
        
    # Function 3... create heatmap of revelant predictors usind recent data
    def heatmap_using_recent_data(self):
        """
        Retrieves the most recent 50000 records from Redis, encodes categorical variables, 
        and creates a heatmap using the variables 'Sex', 'Stroke', 'AgeCategory', 
        'Diabetic', 'Smoking', and 'HeartDisease'. In a real life context where more and
        more data keeps getting added, doctors may want to view the more recent records to 
        see their correlation.

        Returns:
        - heatmap (matplotlib.axes._subplots.AxesSubplot): The heatmap plot.
        """
        try:
            # Retrieve the most recent 50 records from Redis
            recent_data = []
            keys = sorted(self.redis_client.keys(), reverse=True)[:50000]
            for key in keys:
                redis_data = self.redis_client.get(key)
                if redis_data is not None:
                    redis_data_str = redis_data.decode('utf-8')
                    data_dict = json.loads(redis_data_str)
                    recent_data.append(data_dict)
            
            # Create a DataFrame from the recent data
            df_recent = pd.DataFrame(recent_data)

            # Encode categorical variables
            categorical_cols = ['Sex', 'Stroke', 'AgeCategory', 'Diabetic', 'Smoking', 'HeartDisease']
            for col in categorical_cols:
                df_recent[col] = pd.Categorical(df_recent[col])
                df_recent[col] = df_recent[col].cat.codes
            
            # Specify the columns to include in the heatmap
            columns_to_include = ['Sex', 'Stroke', 'AgeCategory', 'Diabetic', 'Smoking', 'HeartDisease']
            df_heatmap = df_recent[columns_to_include]

            # Create the heatmap
            plt.figure(figsize=(10, 8))
            heatmap = sns.heatmap(df_heatmap.corr(), annot=True, cmap='coolwarm', fmt=".2f")
            plt.title('Correlation Heatmap of Recent Data')
            plt.xlabel('Variables')
            plt.ylabel('Variables')
            plt.show()

            return heatmap

        except Exception as e:
            print(f"Error creating heatmap: {e}")
            return None