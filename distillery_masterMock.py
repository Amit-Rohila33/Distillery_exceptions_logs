import os
import asyncio
import json
from distillery_aws import JSONFormatter
import runpod
from runpod import AsyncioEndpoint, AsyncioJob
from contextlib import asynccontextmanager
from unittest.mock import MagicMock
import uuid
import time
import distillery_payloadbuilder
import copy
import sys
import aiohttp
import better_exceptions
import logging

better_exceptions.MAX_LENGTH = None
better_exceptions.hook()

class MockAWSManager(MagicMock):
    @classmethod
    async def get_instance(cls):
        return cls()

    async def close_database_conn(self):
        pass

    async def pop_generation_queue(self):
        pass

    async def push_send_queue(self, *args, **kwargs):
        pass

    def print_log(self, *args, **kwargs):
        pass

# Replace AWSManager with MockAWSManager
aws_manager = MockAWSManager()

try:
    CONFIG = json.load(open('config/config.json'))
except Exception as e:
    print("Failed to load config.json file. Please make sure it exists and is valid. Error:", e)
    exit(1)
APP_NAME = CONFIG['APP_NAME']
SECONDS_PER_TICK = CONFIG['SECONDS_PER_TICK'] # Number of seconds between each tick of the loop
MAX_GENERATIONQUEUE_POP_COUNT=int(CONFIG['MAX_GENERATIONQUEUE_POP_COUNT']) # Maximum number of requests to pop from GenerationQueue at any given time
SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT=int(CONFIG['SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT']) # Multiplier for SECONDS_PER_TICK for each request popped from GenerationQueue
RUNPOD_KEY = os.getenv('RUNPOD_API_KEY')  # Fetch token from environment variable; add this to the environment variables of your system.
INSTANCE_IDENTIFIER = APP_NAME+ '-' + str(uuid.uuid4()) # Unique identifier for this instance of the Master
runpod.api_key=RUNPOD_KEY
MAX_RUNPOD_ATTEMPTS = 3 # Maximum number of attempts to call Runpod
generationqueue_pop_counter = 0 # Counter for the number of requests popped from GenerationQueue

# Function to set tick time based on the number of requests popped from GenerationQueue
def set_tick_time(generationqueue_pop_counter):
    try:
        tick_time = SECONDS_PER_TICK * (1 + SECONDS_PER_TICK_MULTIPLIER_PER_POP_COUNT * generationqueue_pop_counter)
        return tick_time
    except Exception as e:
        raise


# Function to flatten a nested list
def flatten_list(nested_list):
    try:
        flat_list = []
        for item in nested_list:
            if isinstance(item, list):
                flat_list.extend(flatten_list(item))
            else:
                flat_list.append(item)
        return flat_list
    except Exception as e:
        raise


# Function to call Runpod API and return image_list
async def call_runpod(request_id, payload, command_args):
    try:
        for attempt in range(MAX_RUNPOD_ATTEMPTS):
            aws_manager = await MockAWSManager.get_instance()
            if isinstance(payload, str):
                payload = json.loads(payload)
            async with aiohttp.ClientSession() as session:
                endpoint = AsyncioEndpoint(command_args['ENDPOINT_ID'], session)
                job: AsyncioJob = await endpoint.run(payload)
                status = await job.status()
                output = await job.output()
            aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, f"Call_Runpod: Runpod called successfully. Status: {status}, Output: {output}", level='INFO')
            return output
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        line_no = exc_traceback.tb_lineno
        error_message = f'Unhandled error at line {line_no} (attempt {attempt + 1}): {str(e)}'
        if attempt < 2:
            aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, error_message, level='WARNING')
        else:
            aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, error_message, level='ERROR')
            raise


# Function to create routine, call Runpod, and return image URLs
async def create_routine(tuple):
    global generationqueue_pop_counter
    request_id = "N/A"
    try:
        aws_manager = await MockAWSManager.get_instance()
        request_id = tuple[0]
        username = tuple[1]
        generation_input_timestamp = tuple[2]
        payload = tuple[3]
        generation_command_args = tuple[4]
        message_data = tuple[5]
        generation_other_data = tuple[6]
        generation_output_timespentingenerationqueue = tuple[7]
        total_batches = generation_command_args['TOTAL_BATCHES']
        image_urls = []
        starting_seed = int(payload['template_inputs']['NOISE_SEED'])

        async def fetch_image(i):
            local_payload = copy.deepcopy(payload)
            new_seed = starting_seed + i * int(generation_command_args['IMG_PER_BATCH'])
            local_payload['comfy_api'] = distillery_payloadbuilder.PayloadBuilder.update_paths(
                local_payload['comfy_api'], local_payload['noise_seed_template_paths'], str(new_seed))
            local_payload['template_inputs']['NOISE_SEED'] = str(new_seed)
            aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, f"batch {i + 1} of {total_batches} - Sending to Runpod - payload['template_inputs'] = {payload['template_inputs']}",
                                  level='INFO')
            image_files = await call_runpod(request_id, local_payload, generation_command_args)
            return image_files

        tasks = [fetch_image(i) for i in range(total_batches)]
        images = await asyncio.gather(*tasks)
        image_urls = flatten_list(images)
        generation_output = json.dumps(image_urls)
        generation_output_timetogenerateimagefile = time.time() - generation_output_timespentingenerationqueue - generation_input_timestamp
        await aws_manager.push_send_queue(request_id, username, generation_input_timestamp, payload,
                                         generation_command_args, message_data,
                                         generation_other_data, generation_output, generation_output_timespentingenerationqueue,
                                         generation_output_timetogenerateimagefile)
        aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, f"Create_Routine: images pushed to SendQueue.", level='INFO')
    except Exception as e:
        formatted_exception = better_exceptions.format_exception(*sys.exc_info())
        formatted_traceback = ''.join(formatted_exception)
        aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, formatted_traceback, level='ERROR')
    finally:
        generationqueue_pop_counter -= 1
        if generationqueue_pop_counter == MAX_GENERATIONQUEUE_POP_COUNT - 1 and MAX_GENERATIONQUEUE_POP_COUNT > 1:
            aws_manager.print_log("N/A", INSTANCE_IDENTIFIER,
                                  f"Create_Routine: GenerationQueue reduced to below {MAX_GENERATIONQUEUE_POP_COUNT} (MAX_GENERATIONQUEUE_POP_COUNT).",
                                  level='WARNING')
        if generationqueue_pop_counter == 0:
            aws_manager.print_log("N/A", INSTANCE_IDENTIFIER, f"Create_Routine: GenerationQueue reduced to zero.", level='INFO')


# Function to check queue and create routine
async def check_queue_and_create():
    global generationqueue_pop_counter
    request_id = "N/A"
    aws_manager = await MockAWSManager.get_instance()
    while True:
        try:
            tick_time = set_tick_time(generationqueue_pop_counter)
            if generationqueue_pop_counter < MAX_GENERATIONQUEUE_POP_COUNT:
                result = await aws_manager.pop_generation_queue()
                await asyncio.sleep(tick_time)
                if result is not None:
                    generationqueue_pop_counter += 1
                    if generationqueue_pop_counter == MAX_GENERATIONQUEUE_POP_COUNT:
                        aws_manager.print_log("N/A", INSTANCE_IDENTIFIER,
                                              f"MAX_GENERATIONQUEUE_POP_COUNT ({MAX_GENERATIONQUEUE_POP_COUNT}) reached!",
                                              level='WARNING')
                    loop = asyncio.get_event_loop()
                    loop.create_task(create_routine(result))
            else:
                await asyncio.sleep(tick_time)
        except Exception as e:
            formatted_exception = better_exceptions.format_exception(*sys.exc_info())
            formatted_traceback = ''.join(formatted_exception)
            aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, formatted_traceback, level='ERROR')


# Main function
async def main():
    def fix_logs():
        try:
            aiohttp_logger = logging.getLogger('aiohttp')
            custom_formatter = JSONFormatter()  # or SimpleMessageFormatter()
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(custom_formatter)
            aiohttp_logger.addHandler(stream_handler)
            aiohttp_logger.setLevel(logging.INFO)  # Adjust the level as needed
        except Exception as e:
            raise

    try:
        aws_manager = await MockAWSManager.get_instance()
        fix_logs()
        aws_manager.setup_logging()
        loop = asyncio.get_event_loop()
        queue_task = loop.create_task(check_queue_and_create())
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, "Main function cancelled.", level='INFO')
    except Exception as e:
        formatted_exception = better_exceptions.format_exception(*sys.exc_info())
        formatted_traceback = ''.join(formatted_exception)
        aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, formatted_traceback, level='ERROR')
    finally:
        queue_task.cancel()
        try:
            await queue_task
        except asyncio.CancelledError:
            aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, "Queue task cancelled.", level='INFO')
        await aws_manager.close_database_conn()
        aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, "Main function cleanup complete.", level='INFO')

# Running the script
if __name__ == "__main__":
    asyncio.run(main())
