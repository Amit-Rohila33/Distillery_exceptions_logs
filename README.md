## Changes Made in distillery master is contained in distillery_masterMock.py and are also mentioned below.

### 1. Integration of Better-Exceptions

The `better_exceptions` library was integrated to enhance error tracebacks for better debugging. Here's what was done:

- Imported `better_exceptions` and set `MAX_LENGTH` to `None` to enable better exceptions globally.

  ```python
  import better_exceptions
  better_exceptions.MAX_LENGTH = None
  better_exceptions.hook()     # This is something I added in your code `Felipe`, we should wrap the entire script within a better_exceptions context using the better_exceptions.hook() 
  ```

### 2. Mock Database for Testing

A mock database was introduced using the `unittest.mock` module to isolate the testing environment from actual database connections. The `MockAWSManager` class was created with mocked methods for testing purposes.

```python
from unittest.mock import MagicMock

class MockAWSManager(MagicMock):
    # There are some random methods which I found on google `Felipe`, I just added them, you can ignore them.
```

The usage of `AWSManager` in the code was then replaced with `MockAWSManager` during testing to prevent actual database interactions.

### 3. Code Modifications

The code was updated with the following changes:

- Replaced instances of `AWSManager` with `MockAWSManager` for testing.

  ```python
  aws_manager = MockAWSManager()
  ```

- Adapted the `create_routine` function to handle the mock database.

  ```python
  async def create_routine(tuple):
      global generationqueue_pop_counter
      request_id = "N/A"
      try:
          aws_manager = await MockAWSManager.get_instance()
          # ... (rest of the function remains unchanged)
      except Exception as e:
          formatted_exception = better_exceptions.format_exception(*sys.exc_info())
          formatted_traceback = ''.join(formatted_exception)
          aws_manager.print_log(request_id, INSTANCE_IDENTIFIER, formatted_traceback, level='ERROR')
      finally:
          generationqueue_pop_counter -= 1
          # ... (rest of the function remains unchanged)
  ```

- Updated the `main` function to use `MockAWSManager` and introduced a `fix_logs` function for log formatting.

  ```python
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
          # ........ rest code is not changed
      except asyncio.CancelledError:
          aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, "Main function cancelled.", level='INFO')
      except Exception as e:
          formatted_exception = better_exceptions.format_exception(*sys.exc_info())
          formatted_traceback = ''.join(formatted_exception)
          aws_manager.print_log('N/A', INSTANCE_IDENTIFIER, formatted_traceback, level='ERROR')
      finally:
          # ....... rest code is not changed
  ```


  `Felipe`, I was just going through my code and just noticed that `better_exceptions.hook()` as added should work, maybe if it could all the shit I have done above is senseless
