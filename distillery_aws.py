import boto3
from watchtower import CloudWatchLogHandler
import aioboto3
from typing import List, Tuple
import os
import logging
import inspect
import time
from io import BytesIO
import aiomysql
import json
import time
import socket
import aiomcache 
import pytz
import datetime
import asyncio
from botocore.exceptions import ClientError
import copy
import better_exceptions
better_exceptions.MAX_LENGTH = None

try:
    CONFIG = json.load(open('config/config.json'))
except Exception as e:
    print("Failed to load config.json file. Please make sure it exists and is valid. Error:", e)
    exit(1)
APP_NAME = CONFIG['APP_NAME']
VALID_COUNTERS = CONFIG["VALID_GENERATION_COUNTERS_LIST"]
AWS_REGION_NAME = os.getenv('AWS_REGION_NAME')
AWS_LOG_GROUP = os.getenv('AWS_LOG_GROUP')
AWS_LOG_STREAM_NAME = os.getenv('AWS_LOG_STREAM_NAME')
AWS_S3_BUCKET_NAME = os.getenv('AWS_S3_BUCKET_NAME')
AWS_S3_ACCESS_KEY = os.getenv('AWS_S3_ACCESS_KEY')
AWS_S3_SECRET_KEY = os.getenv('AWS_S3_SECRET_KEY')
AWS_RDS_HOST_NAME = os.getenv('AWS_RDS_HOST_NAME')
AWS_RDS_USER_NAME = os.getenv('AWS_RDS_USER_NAME')
AWS_RDS_USER_PASSWORD = os.getenv('AWS_RDS_USER_PASSWORD')
AWS_RDS_NAME = os.getenv('AWS_RDS_NAME')
AWS_MEMCACHED_HOST = os.getenv('AWS_MEMCACHED_HOST') # AWS Memcached host
AWS_MEMCACHED_PORT = int(os.getenv('AWS_MEMCACHED_PORT')) # AWS Memcached port
AWS_MEMCACHED_TIMEOUT_TIME = int(os.getenv('AWS_MEMCACHED_TIMEOUT_TIME')) # Timeout time for AWS memcached lock
RETRY_ATTEMPTS=5 # Number of times to retry creating a connection pool
if not all([AWS_RDS_HOST_NAME, AWS_RDS_USER_NAME, AWS_RDS_USER_PASSWORD, AWS_RDS_NAME]):
    print("One or more database configuration environment variables are missing. Please set all of them.")
    exit(1)

LAST_QUEUEPOP_TIME = 0 # Last time a queuepop was done; initialized to 0
QUEUEPOP_HEARTBEAT_INTERVAL = 600 # Interval between empty queuepop prints, in seconds
TIMEZONE = pytz.timezone('US/Eastern')

def validate_generation_counter(name_of_counter):
    if name_of_counter not in VALID_COUNTERS:
        raise ValueError(f"Invalid generation count name: {name_of_counter}")

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = record.__dict__.copy()
        # Check if 'msg' is already in JSON format
        try:
            log_dict = json.loads(log_record['msg'])
        except Exception as e:
            log_dict = copy.deepcopy(log_record['msg'])
        log_dict['level'] = record.levelname
        # return the JSON string
        return json.dumps(log_dict)
    
class SimpleMessageFormatter(logging.Formatter):
    def format(self, record):
        log_record = record.__dict__.copy()
        # Check if 'msg' is already in JSON format
        try:
            log_dict = json.loads(log_record['msg'])
        except Exception as e:
            log_dict = copy.deepcopy(log_record['msg'])
        log_dict['level'] = record.levelname
        # return the simplified message
        level = log_dict['level']
        esttime = log_dict['esttime']
        message = log_dict['message']
        return f"{esttime} - {level} - {message}"

class AWSManager:
    _instance = None
    _memcached = None
    _instance_lock = asyncio.Lock()

    def __new__(cls):
        try:
            if not cls._instance:
                cls._instance = super().__new__(cls)
                cls._instance.region_name = AWS_REGION_NAME
                cls._instance.log_group = AWS_LOG_GROUP
                cls._instance.log_stream_name = AWS_LOG_STREAM_NAME
                cls._instance.db_instance = None
                cls._instance.setup_logging()
            return cls._instance
        except Exception as e:
            raise

    def setup_logging(self, level=logging.INFO): 
        try:
            app_logger = logging.getLogger(APP_NAME)
            if not app_logger.hasHandlers():  # Check if handlers are already added
                for handler in logging.root.handlers[:]: # Remove existing handlers from root logger
                    logging.root.removeHandler(handler)
                app_logger.setLevel(level)
                # Setup CloudWatch log handler
                session = boto3.Session(region_name=self.region_name)
                cloudwatch_client = session.client('logs')
                cw_handler = CloudWatchLogHandler(boto3_client=cloudwatch_client, log_group=self.log_group, stream_name=self.log_stream_name)
                json_formatter = JSONFormatter()
                cw_handler.setFormatter(json_formatter)
                app_logger.addHandler(cw_handler)
                # Setup StreamHandler
                stream_handler = logging.StreamHandler()
                simple_message_formatter = SimpleMessageFormatter()
                stream_handler.setFormatter(simple_message_formatter)
                app_logger.addHandler(stream_handler)
                app_logger.propagate = True
        except Exception as e:
            raise

    def print_log(self, request_id, context, message, level='INFO'): 
        try:
            app_logger = logging.getLogger(APP_NAME)
            caller_frame = inspect.currentframe().f_back
            script_name = os.path.basename(caller_frame.f_globals["__file__"])
            line_number = caller_frame.f_lineno
            function_name = caller_frame.f_code.co_name
            hostname = socket.gethostname()
            # Convert timestamp to EST
            unixtime = time.time()
            utc_time = datetime.datetime.utcfromtimestamp(unixtime)
            est_time = utc_time.replace(tzinfo=pytz.utc).astimezone(pytz.timezone('US/Eastern'))
            log_data = {
                "context": context,
                "esttime": est_time.strftime("%Y-%m-%d %H:%M:%S"),  # EST timestamp
                "unixtime": unixtime,  # Unix timestamp
                "request_id": request_id,
                "message": message,
                "script_name": script_name,
                "function_name": function_name,
                "line_number": line_number,
                "hostname": f"{APP_NAME}-{hostname}"
            }
            message_to_print = json.dumps(log_data)
            if level == 'INFO':
                app_logger.info(message_to_print)
            elif level == 'ERROR':
                app_logger.error(message_to_print)
            elif level == 'WARNING':
                app_logger.warning(message_to_print)
        except Exception as e:
            raise

    @classmethod
    async def get_instance(cls):
        try:
            async with cls._instance_lock:  # Ensure that only one coroutine can perform this at a time
                if not cls._instance:
                    cls._instance = await cls._create_new_instance()
                else:
                    try: # Test acquiring a connection from the pool
                        async with cls._instance.pool.acquire() as conn:
                            pass  # If this succeeds, the pool is operational
                    except Exception as e:
                        error_message = f"Failed to acquire a connection. The pool may be closed or not operational. Exception: {e}"
                        cls._instance.print_log("N/A", APP_NAME, error_message, "ERROR")
                        cls._instance.pool = await cls._instance.create_database_connection() # The pool is not operational, recreate it
            return cls._instance
        except Exception as e:
            raise

    @classmethod
    async def _create_new_instance(cls):
        try:
            instance = cls()
            instance.pool = await instance.create_database_connection()
            return instance
        except Exception as e:
            raise

    async def create_database_connection(self):
        for _ in range(RETRY_ATTEMPTS):  # Retry logic for creating connection pool
            try:
                pool = await aiomysql.create_pool(
                    host=AWS_RDS_HOST_NAME,
                    user=AWS_RDS_USER_NAME,
                    password=AWS_RDS_USER_PASSWORD,
                    db=AWS_RDS_NAME
                )
                self.print_log("N/A", APP_NAME, "Connection to MySQL successful!", "INFO")
                return pool
            except Exception as e:
                if _ < RETRY_ATTEMPTS - 1:
                    self.print_log("N/A", APP_NAME, f"Error creating MySQL connection pool: {e}", "WARNING")
                    await asyncio.sleep(1)  # Wait a bit before retrying
                else:
                    self.print_log("N/A", APP_NAME, f"Error creating MySQL connection pool: {e}", "ERROR")
        raise Exception("Failed to create a MySQL connection pool after several attempts")

    def file_exists_in_s3(self, key): # Note that it's not an async function
        s3 = boto3.client('s3', aws_access_key_id=AWS_S3_ACCESS_KEY, aws_secret_access_key=AWS_S3_SECRET_KEY, region_name=AWS_REGION_NAME)
        try:
            s3.head_object(Bucket=AWS_S3_BUCKET_NAME, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise

    async def upload_fileobj(self, files: List[Tuple[BytesIO, str]]):
        try:
            session = aioboto3.Session(aws_access_key_id=AWS_S3_ACCESS_KEY, aws_secret_access_key=AWS_S3_SECRET_KEY, region_name=AWS_REGION_NAME)
            async with session.client('s3') as s3:
                
                for file_obj, key in files:
                    file_obj.seek(0)  # Ensure we're at the start of the file
                    await s3.upload_fileobj(file_obj, AWS_S3_BUCKET_NAME, key)
        except Exception as e:
            raise

    async def download_fileobj(self, keys: List[str]) -> List[BytesIO]:  # keys is a list of keys
        try:
            file_objs = []
            session = aioboto3.Session(aws_access_key_id=AWS_S3_ACCESS_KEY, aws_secret_access_key=AWS_S3_SECRET_KEY, region_name=AWS_REGION_NAME)
            async with session.client('s3') as s3:
                for key in keys:
                    file_obj = BytesIO()
                    await s3.download_fileobj(AWS_S3_BUCKET_NAME, key, file_obj)
                    file_obj.seek(0)  # Ensure we're at the start of the file
                    file_objs.append(file_obj)
            return file_objs
        except Exception as e:
            raise
    
    async def close_database_conn(self): # Closes the database connection
        try:
            self.pool.close()
            await self.pool.wait_closed()
        except Exception as e:
            raise

    async def execute_query(self, query, params=None, should_commit=True):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    if params:
                        await cursor.execute(query, params)
                    else:
                        await cursor.execute(query)
                    if should_commit:
                        await conn.commit()
        except Exception as e:
            try: # Attempt to rollback 
                if should_commit:
                    await conn.rollback()
            except Exception as e:
                raise
            raise

    async def read_query(self, query, params=None, should_commit=True):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    if params:
                        await cursor.execute(query, params)
                    else:
                        await cursor.execute(query)
                    result = await cursor.fetchall()
                    if should_commit:
                        await conn.commit()
                    return result
        except Exception as e:
            try: # Attempt to rollback
                if should_commit:
                    await conn.rollback()
            except Exception as e:
                raise
            raise

    async def validate_user(self, username): # Checks if a user exists in the database; if not, creates a new user
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                        await conn.begin()
                        query = "SELECT username FROM Users WHERE username = %s"
                        await cursor.execute(query, (username,))
                        result = await cursor.fetchall()
                        if result:
                            pass
                        else:
                            self.print_log("N/A", APP_NAME, f"Creating user: {username}", "INFO")
                            user_date_created = time.time()
                            query = """
                            INSERT INTO Users (username, user_date_created, number_of_generations, number_of_free_generations_today, number_of_paid_generations_this_month, number_of_generations_this_minute, number_of_generations_this_hour, number_of_generations_this_day, number_of_generations_this_week, number_of_generations_this_month, number_of_throttling_events, number_of_graylist_events, number_of_blacklist_events)
                            VALUES (%s, FROM_UNIXTIME(%s), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
                            """
                            await cursor.execute(query, (username, user_date_created))
                            await conn.commit()  # Moved the commit statement here
        except Exception as e:
            raise

    async def update_generations(self, username):
        try:
            query = """
            UPDATE Users 
            SET 
                number_of_generations = number_of_generations + 1,
                number_of_generations_this_minute = number_of_generations_this_minute + 1,
                number_of_generations_this_hour = number_of_generations_this_hour + 1,
                number_of_generations_this_day = number_of_generations_this_day + 1,
                number_of_generations_this_week = number_of_generations_this_week + 1,
                number_of_generations_this_month = number_of_generations_this_month + 1
            WHERE username = %s;
            """
            await self.execute_query(query, (username,))
        except Exception as e:
            raise

    async def check_generation_counter(self, username, name_of_counter):
        try:
            validate_generation_counter(name_of_counter)
            self.print_log("N/A", APP_NAME, f"Checking number of generations of type '{name_of_counter}' for user: {username}", "INFO")
            query = f"""
            SELECT {name_of_counter}
            FROM Users 
            WHERE username = %s;
            """
            results = await self.read_query(query, (username,))
            return results[0][0] if results else None
        except Exception as e:
            raise

    async def update_free_generations(self, username):
        try:
            query = """
            UPDATE Users 
            SET number_of_free_generations_today = number_of_free_generations_today + 1
            WHERE username = %s;
            """
            await self.execute_query(query, (username,))   
        except Exception as e:
            raise

    async def update_paid_generations(self, username):
        try:
            query = """
            UPDATE Users 
            SET number_of_paid_generations_this_month = number_of_paid_generations_this_month + 1
            WHERE username = %s;
            """
            await self.execute_query(query, (username,))   
        except Exception as e:
            raise

    async def update_throttling_events(self, username):
        try:
            query = """
            UPDATE Users 
            SET number_of_throttling_events = number_of_throttling_events + 1
            WHERE username = %s;
            """
            await self.execute_query(query, (username,))   
        except Exception as e:
            raise

    async def update_graylist_events(self, username):
        try:
            self.print_log("N/A", APP_NAME, f"Updating graylist events for user: {username}", "INFO")
            query = """
            UPDATE Users 
            SET number_of_graylist_events = number_of_graylist_events + 1
            WHERE username = %s;
            """
            await self.execute_query(query, (username,))
        except Exception as e:
            raise

    async def update_blacklist_events(self, username):
        try:
            self.print_log("N/A", APP_NAME, f"Updating blacklist events for user: {username}", "INFO")
            query = """
            UPDATE Users 
            SET number_of_blacklist_events = number_of_blacklist_events + 1
            WHERE username = %s;
            """
            await self.execute_query(query, (username,))   
        except Exception as e:
            raise

    async def reset_generation_counter(self, name_of_counter):
        try:
            validate_generation_counter(name_of_counter)                    
            query = f"""
            UPDATE Users 
            SET {name_of_counter} = 0;
            """
            await self.execute_query(query)
        except Exception as e:
            raise

    async def push_graylist_log(self, generation_request_id, username, generation_input_timestamp, command_string, generation_command_args):
        try:
            generation_command_args_str = json.dumps(generation_command_args)
            query = """
            INSERT INTO GraylistLog (generation_request_id, username, generation_input_timestamp, command_string, generation_command_args)
            VALUES (%s, %s, %s, %s, %s);
            """
            await self.execute_query(query, (generation_request_id, username, generation_input_timestamp, command_string, generation_command_args_str))
        except Exception as e:
            raise
        
    async def push_blacklist_log(self, generation_request_id, username, generation_input_timestamp, command_string, generation_command_args):
        try:
            generation_command_args_str = json.dumps(generation_command_args)
            query = """
            INSERT INTO BlacklistLog (generation_request_id, username, generation_input_timestamp, command_string, generation_command_args)
            VALUES (%s, %s, %s, %s, %s);
            """
            await self.execute_query(query, (generation_request_id, username, generation_input_timestamp, command_string, generation_command_args_str))
        except Exception as e:
            raise

    async def push_generation_log(self, generation_request_id, username, generation_input_timestamp, generation_input, generation_command_args, 
                                  message_data, generation_other_data, generation_output, generation_output_timespentingenerationqueue, generation_output_timetogenerateimagefile, 
                                  generation_output_timespentinsendqueue, generation_output_timetosend, reported_elapsed_time):
        try:
            generation_input_str = json.dumps(generation_input)
            generation_command_args_str = json.dumps(generation_command_args)
            message_data_str = json.dumps(message_data)
            generation_other_data_str = json.dumps(generation_other_data)
            generation_output_str = json.dumps(generation_output)
            query = """
            INSERT INTO GenerationLog (generation_request_id, username, generation_input_timestamp, generation_input, generation_command_args, message_data, generation_other_data, generation_output, generation_output_timespentingenerationqueue, generation_output_timetogenerateimagefile, generation_output_timespentinsendqueue, generation_output_timetosend, generation_output_reportedelapsedtime)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            await self.execute_query(query, (generation_request_id, username, generation_input_timestamp, generation_input_str, generation_command_args_str, 
                                            message_data_str, generation_other_data_str, generation_output_str, generation_output_timespentingenerationqueue, generation_output_timetogenerateimagefile, 
                                            generation_output_timespentinsendqueue, generation_output_timetosend, reported_elapsed_time))
        except Exception as e:
            raise
        
    async def push_generation_queue(self, generation_request_id, username, generation_input_timestamp, generation_input, generation_command_args, message_data, generation_other_data):
        try:
            generation_input_str = json.dumps(generation_input)
            generation_command_args_str = json.dumps(generation_command_args)
            message_data_str = json.dumps(message_data)
            generation_other_data_str = json.dumps(generation_other_data)
            query = """
            INSERT INTO GenerationQueue (generation_request_id, username, generation_input_timestamp, generation_input, generation_command_args, message_data, generation_other_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            await self.execute_query(query, (generation_request_id, username, generation_input_timestamp, generation_input_str, generation_command_args_str, message_data_str, generation_other_data_str))
        except Exception as e:
            raise

    async def pop_generation_queue(self):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await conn.begin()
                    select_query = """
                    SELECT * FROM GenerationQueue 
                    ORDER BY generation_input_timestamp ASC 
                    LIMIT 1 FOR UPDATE;
                    """
                    await cursor.execute(select_query)
                    results = await cursor.fetchall()
                    if results:
                        generation_queue_entry = results[0]
                        delete_query = """
                        DELETE FROM GenerationQueue 
                        WHERE generation_request_id = %s;
                        """
                        await cursor.execute(delete_query, (generation_queue_entry[0],))
                        print(f"{time.time():.2f} - GenerationQueue popped 1 entry.")
                    else:
                        global LAST_QUEUEPOP_TIME
                        if time.time() - LAST_QUEUEPOP_TIME > QUEUEPOP_HEARTBEAT_INTERVAL:
                            timestamp = datetime.datetime.fromtimestamp(time.time(), tz=pytz.utc).astimezone(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
                            print(f"{timestamp} - GenerationQueue is empty.")  # Not printed to log in order to avoid spamming the log file
                            LAST_QUEUEPOP_TIME = time.time()
                    await conn.commit()
                    if results:
                        generation_queue_entry = list(generation_queue_entry)
                        generation_queue_entry[3] = json.loads(generation_queue_entry[3])
                        generation_queue_entry[4] = json.loads(generation_queue_entry[4])
                        generation_queue_entry[5] = json.loads(generation_queue_entry[5])
                        generation_queue_entry[6] = json.loads(generation_queue_entry[6])
                        generation_queue_entry.append(time.time() - generation_queue_entry[2]) # adds time spent in GenerationQueue until popped
                        return tuple(generation_queue_entry)
                    else:
                        return None
        except Exception as e:
            raise

    async def push_send_queue(self, generation_request_id, username, generation_input_timestamp, generation_input, generation_command_args, 
                              message_data, generation_other_data, generation_output, generation_output_timespentingenerationqueue, generation_output_timetogenerateimagefile):
        try:
            generation_input_str = json.dumps(generation_input)
            generation_command_args_str = json.dumps(generation_command_args)
            message_data_str = json.dumps(message_data)
            generation_other_data_str = json.dumps(generation_other_data)
            generation_output_str = json.dumps(generation_output)
            query = """
            INSERT INTO SendQueue (generation_request_id, username, generation_input_timestamp, generation_input, generation_command_args, message_data, generation_other_data, generation_output, generation_output_timespentingenerationqueue, generation_output_timetogenerateimagefile)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            await self.execute_query(query, (generation_request_id, username, generation_input_timestamp, generation_input_str, generation_command_args_str, 
                                            message_data_str, generation_other_data_str, generation_output_str, generation_output_timespentingenerationqueue, generation_output_timetogenerateimagefile))
        except Exception as e:
            raise

    async def pop_send_queue(self):
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await conn.begin()
                    select_query = """
                    SELECT * FROM SendQueue 
                    ORDER BY generation_input_timestamp ASC 
                    LIMIT 1 FOR UPDATE;
                    """
                    await cursor.execute(select_query)
                    results = await cursor.fetchall()
                    if results:
                        send_queue_entry = results[0]
                        delete_query = """
                        DELETE FROM SendQueue 
                        WHERE generation_request_id = %s;
                        """
                        await cursor.execute(delete_query, (send_queue_entry[0],))
                        print(f"{time.time():.2f} - SendQueue popped 1 entry.")
                    else:
                        global LAST_QUEUEPOP_TIME
                        if time.time() - LAST_QUEUEPOP_TIME > QUEUEPOP_HEARTBEAT_INTERVAL:
                            timestamp = datetime.datetime.fromtimestamp(time.time(), tz=pytz.utc).astimezone(TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
                            print(f"{timestamp} - SendQueue is empty.")  # Not printed to log in order to avoid spamming the log file
                            LAST_QUEUEPOP_TIME = time.time()
                    await conn.commit()
                    if results:
                        send_queue_entry = list(send_queue_entry)
                        send_queue_entry[3] = json.loads(send_queue_entry[3])
                        send_queue_entry[4] = json.loads(send_queue_entry[4])
                        send_queue_entry[5] = json.loads(send_queue_entry[5])
                        send_queue_entry[6] = json.loads(send_queue_entry[6])
                        send_queue_entry[7] = json.loads(send_queue_entry[7])
                        send_queue_entry.append(time.time() - send_queue_entry[2] - send_queue_entry[8] - send_queue_entry[9]) # adds time spent in SendQueue until popped
                        return tuple(send_queue_entry)
                    else:
                        return None
        except Exception as e:
            raise

    async def check_lora_model_exists(self, lora_model_name):
        try:
            query = """
            SELECT COUNT(*) 
            FROM LoraModelList 
            WHERE lora_model_name = %s;
            """
            result = await self.read_query(query, (lora_model_name,))
            return result[0][0] > 0
        except Exception as e:
            raise
    
    async def add_lora_model(self, lora_model_name, lora_model_filename, lora_model_type, 
                            lora_model_default_weight_model, lora_model_default_weight_clip, 
                            lora_model_is_private):
        try:
            query = """
            INSERT INTO LoraModelList (lora_model_name, lora_model_filename, lora_model_type, 
                                    lora_model_default_weight_model, lora_model_default_weight_clip, 
                                    lora_model_is_private) 
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            params = (lora_model_name, lora_model_filename, lora_model_type, 
                    lora_model_default_weight_model, lora_model_default_weight_clip, 
                    lora_model_is_private)
            await self.execute_query(query, params)
        except Exception as e:
            raise
    
    async def delete_lora_model(self, lora_model_name):
        try:
            query = """
            DELETE FROM LoraModelList 
            WHERE lora_model_name = %s;
            """
            await self.execute_query(query, (lora_model_name,))
        except Exception as e:
            raise

    async def check_lora_model_is_private(self, lora_model_name):
        try:
            query = """
            SELECT lora_model_is_private 
            FROM LoraModelList 
            WHERE lora_model_name = %s;
            """
            result = await self.read_query(query, (lora_model_name,))
            return result[0][0] == 1
        except Exception as e:
            raise

    async def check_lora_credentials_exist(self, lora_model_name, username):
        try:
            query = """
            SELECT COUNT(*) 
            FROM LoraPrivateCredentials 
            WHERE lora_model_name = %s AND username = %s;
            """
            result = await self.read_query(query, (lora_model_name, username))
            return result[0][0] > 0
        except Exception as e:
            raise

    async def add_lora_credentials(self, lora_model_name, username):
        try:
            query = """
            INSERT INTO LoraPrivateCredentials (lora_model_name, username) 
            VALUES (%s, %s);
            """
            await self.execute_query(query, (lora_model_name, username))
        except Exception as e:
            raise

    async def delete_lora_credentials(self, lora_model_name, username):
        try:
            query = """
            DELETE FROM LoraPrivateCredentials 
            WHERE lora_model_name = %s AND username = %s;
            """
            await self.execute_query(query, (lora_model_name, username))
        except Exception as e:
            raise

    async def add_distillery_command(self, distillery_command_name):
        try:
            query = """
            INSERT INTO DistilleryCommands (distillery_command_name) 
            VALUES (%s);
            """
            await self.execute_query(query, (distillery_command_name,))
        except Exception as e:
            raise

    async def delete_distillery_command(self, distillery_command_name):
        try:
            query = """
            DELETE FROM DistilleryCommands 
            WHERE distillery_command_name = %s;
            """
            await self.execute_query(query, (distillery_command_name,))
        except Exception as e:
            raise

    async def create_lora_acceptance_group(self, lora_group_name):
        try:
            query = """
            INSERT INTO LoraAcceptanceGroups (lora_group_name) 
            VALUES (%s);
            """
            await self.execute_query(query, (lora_group_name,))
        except Exception as e:
            raise

    async def delete_lora_acceptance_group(self, lora_group_name):
        try:
            query = """
            DELETE FROM LoraAcceptanceGroups 
            WHERE lora_group_name = %s;
            """
            await self.execute_query(query, (lora_group_name,))
        except Exception as e:
            raise

    async def map_group_to_command(self, lora_group_name, distillery_command_name):
        try:
            query = """
            INSERT INTO LoraCommandGroupMapping (lora_group_name, distillery_command_name) 
            VALUES (%s, %s);
            """
            await self.execute_query(query, (lora_group_name, distillery_command_name))
        except Exception as e:
            raise

    async def unmap_group_from_command(self, lora_group_name, distillery_command_name):
        try:
            query = """
            DELETE FROM LoraCommandGroupMapping 
            WHERE lora_group_name = %s AND distillery_command_name = %s;
            """
            await self.execute_query(query, (lora_group_name, distillery_command_name))
        except Exception as e:
            raise

    async def add_lora_to_group(self, lora_model_name, lora_group_name):
        try:
            query = """
            INSERT INTO LoraGroupMapping (lora_model_name, lora_group_name) 
            VALUES (%s, %s);
            """
            await self.execute_query(query, (lora_model_name, lora_group_name))
        except Exception as e:
            raise

    async def remove_lora_from_group(self, lora_model_name, lora_group_name):
        try:
            query = """
            DELETE FROM LoraGroupMapping 
            WHERE lora_model_name = %s AND lora_group_name = %s;
            """
            await self.execute_query(query, (lora_model_name, lora_group_name))
        except Exception as e:
            raise

    async def get_lora_models_by_group(self, lora_group_name):
        try:
            query = """
            SELECT lml.lora_model_name, lml.lora_model_filename, lml.lora_model_type, 
                lml.lora_model_default_weight_model, lml.lora_model_default_weight_clip, 
                lml.lora_model_is_private
            FROM LoraModelList lml
            INNER JOIN LoraGroupMapping lgm ON lml.lora_model_name = lgm.lora_model_name
            WHERE lgm.lora_group_name = %s;
            """
            lora_models_data = await self.read_query(query, (lora_group_name,))
            # Building the dictionary
            lora_models_dict = {}
            for model in lora_models_data:
                model_name, model_filename, model_type, default_weight_model, default_weight_clip, is_private = model
                lora_models_dict[model_name] = {
                    "MODEL_FILENAME": model_filename,
                    "MODEL_TYPE": model_type,
                    "DEFAULT_WEIGHT_MODEL": default_weight_model,
                    "DEFAULT_WEIGHT_CLIP": default_weight_clip,
                    "IS_PRIVATE": is_private
                }
            return lora_models_dict
        except Exception as e:
            raise
        
    async def get_allowed_loras_for_user(self, username, lora_group_name, command_name_in_database):
        try:
            # Query to fetch Lora models and their details in the specified group
            query = """
            SELECT lml.lora_model_name, lml.lora_model_filename, lml.lora_model_type, 
                lml.lora_model_default_weight_model, lml.lora_model_default_weight_clip, 
                lml.lora_model_is_private
            FROM LoraModelList lml
            INNER JOIN LoraGroupMapping lgm ON lml.lora_model_name = lgm.lora_model_name
            INNER JOIN LoraCommandGroupMapping lcg ON lgm.lora_group_name = lcg.lora_group_name
            WHERE lgm.lora_group_name = %s AND lcg.distillery_command_name = %s;
            """ # this query does the following: fetches all Lora models in the specified group, then filters based on the specified command
            lora_models_data = await self.read_query(query, (lora_group_name, command_name_in_database))
            # Dictionary to store allowed Loras
            allowed_loras = {}
            # Filter based on private status and user credentials
            for model in lora_models_data:
                model_name, model_filename, model_type, default_weight_model, default_weight_clip, is_private = model            
                if not is_private:
                    allowed_loras[model_name] = {
                        "MODEL_FILENAME": model_filename,
                        "MODEL_TYPE": model_type,
                        "DEFAULT_WEIGHT_MODEL": default_weight_model,
                        "DEFAULT_WEIGHT_CLIP": default_weight_clip,
                        "IS_PRIVATE": is_private
                    }
                else:
                    # Check if the user has credentials for this private Lora model
                    credential_check_query = """
                    SELECT COUNT(*) 
                    FROM LoraPrivateCredentials 
                    WHERE lora_model_name = %s AND username = %s;
                    """
                    credential_result = await self.read_query(credential_check_query, (model_name, username))
                    if credential_result[0][0] > 0:  # User has credentials
                        allowed_loras[model_name] = {
                            "MODEL_FILENAME": model_filename,
                            "MODEL_TYPE": model_type,
                            "DEFAULT_WEIGHT_MODEL": default_weight_model,
                            "DEFAULT_WEIGHT_CLIP": default_weight_clip,
                            "IS_PRIVATE": is_private
                        }
            return allowed_loras
        except Exception as e:
            raise

    class Memcached:
        @staticmethod
        async def get_memcached_connection():
            try:
                if AWSManager._memcached is None:
                    AWSManager._memcached = aiomcache.Client(AWS_MEMCACHED_HOST, AWS_MEMCACHED_PORT)
                return AWSManager._memcached
            except Exception as e:
                raise

        @staticmethod
        async def get_lock(request_id):
            try:
                memcached = await AWSManager.Memcached.get_memcached_connection()
                hostname = socket.gethostname().encode('utf-8')
                success = await memcached.add(request_id.encode('utf-8'), hostname, exptime=AWS_MEMCACHED_TIMEOUT_TIME)
                return success == 1
            except Exception as e:
                raise