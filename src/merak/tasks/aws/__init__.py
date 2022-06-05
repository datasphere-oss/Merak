"""
This module contains a collection of tasks for interacting with AWS resources.

All AWS related tasks can be authenticated using the `AWS_CREDENTIALS` merak Secret that should be a dictionary with two keys: `"ACCESS_KEY"` and `"SECRET_ACCESS_KEY"`.  See [Third Party Authentication](../../../orchestration/recipes/third_party_auth.html) for more information.
"""
try:
    from merak.tasks.aws.s3 import S3Download, S3Upload, S3List
    from merak.tasks.aws.lambda_function import (
        LambdaCreate,
        LambdaDelete,
        LambdaInvoke,
        LambdaList,
    )
    from merak.tasks.aws.step_function import StepActivate
    from merak.tasks.aws.secrets_manager import AWSSecretsManager
    from merak.tasks.aws.parameter_store_manager import AWSParametersManager
    from merak.tasks.aws.batch import BatchSubmit
    from merak.tasks.aws.client_waiter import AWSClientWait
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.aws` requires merak to be installed with the "aws" extra.'
    ) from err

__all__ = [
    "AWSClientWait",
    "AWSSecretsManager",
    "AWSParametersManager",
    "BatchSubmit",
    "LambdaCreate",
    "LambdaDelete",
    "LambdaInvoke",
    "LambdaList",
    "S3Download",
    "S3List",
    "S3Upload",
    "StepActivate",
]
