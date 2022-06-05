"""
This is a collection of tasks to interact with a Cube.js or Cube Cloud environment.
"""

try:
    from merak.tasks.cubejs.cubejs_tasks import CubeJSQueryTask
except ImportError as err:
    raise ImportError(
        'merak.tasks.cubejs` requires merak to be installed with the "cubejs" extra.'
    ) from err
