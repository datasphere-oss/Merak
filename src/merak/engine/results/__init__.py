"""
`Result` subclasses are the backbone of tracking the value, type and optional persistence method of return values from tasks by exposing a `read` / `write` / `exists` interface for common storage backends. Results can also be instantiated directly by the user in a task to use those methods to interact with persistent storage and track data besides a task's return value.

A results `read` / `write` / `exists` methods depends on a result's `location` attribute, which can be a concrete string or a templated string that will be formatted at time of `write` using `merak.context`.

Note that a result's `read` and `write` methods return new `Result` instances, the former with their `location` attribute formatted, and the latter with the `value` attribute hydrated.

For example, here is how you would use a result in a task directly to read and write arbitrary pieces of data:

```python
import merak
from merak import task
from merak.engine.results import S3Result

MY_RESULTS = S3Result(bucket='my_bucket', location="{task_name}.txt")

@task(name="my_example_task")
def my_example_task():
    # read data from a file in the bucket.
    my_task_data = MY_RESULTS.read(location="some_data_in_my_bucket.csv")
    print(my_task_data.value) # is the deserialized data that was in the file s3://my_bucket/some_data_in_my_bucket.csv

    # write data to the templated location in the bucket using merak context
    data = 3
    my_task_data = MY_RESULTS.write(data, **merak.context)
    print(my_task_data.value) # is the value `3
    print(my_task_data.location) # is "my_example_task.txt"

```

Results will only persist return data if checkpointing is turned on. To learn more about how to use results and how to configure checkpointing, read our tutorial on [Using Results](/core/advanced_tutorials/using-results.md).


"""
from merak.engine.results.constant_result import ConstantResult
from merak.engine.results.gcs_result import GCSResult
from merak.engine.results.local_result import LocalResult
from merak.engine.results.merak_result import merakResult
from merak.engine.results.azure_result import AzureResult
from merak.engine.results.s3_result import S3Result
from merak.engine.results.secret_result import SecretResult

__all__ = [
    "AzureResult",
    "ConstantResult",
    "GCSResult",
    "LocalResult",
    "merakResult",
    "S3Result",
    "SecretResult",
]
