"""
This module contains a collection of tasks to interact with Neo4j.
"""

try:
    from merak.tasks.neo4j.neo4j_tasks import Neo4jRunCypherQueryTask
except ImportError as err:
    raise ImportError(
        'Using `merak.tasks.neo4j` requires merak to be installed with the "neo4j" extra.'
    ) from err

__all__ = ["Neo4jRunCypherQueryTask"]
