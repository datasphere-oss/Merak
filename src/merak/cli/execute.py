import click

import merak
from merak.client import Client
from merak.engine import get_default_flow_runner_class
from merak.tasks.secrets import merakSecret
from merak.utilities.graphql import with_args


@click.group(hidden=True)
def execute():
    """
    Execute flow runs.

    \b
    Usage:
        $ merak execute [OBJECT]

    \b
    Arguments:
        flow-run  Execute a flow run with a backend API

    \b
    Examples:
        $ merak execute flow-run
    """


@execute.command(hidden=True)
def flow_run():
    """
    Execute a flow run in the context of a backend API.

    TODO: Either deprecate this and replace with a new CLI command that calls
          `merak.backend.execute_flow_run` or update this to call that command
          with the flow_run_id pulled from context
    """
    flow_run_id = merak.context.get("flow_run_id")
    if not flow_run_id:
        click.echo("Not currently executing a flow within a Cloud context.")
        raise Exception("Not currently executing a flow within a Cloud context.")

    query = {
        "query": {
            with_args("flow_run", {"where": {"id": {"_eq": flow_run_id}}}): {
                "flow": {"name": True, "storage": True, "run_config": True},
                "version": True,
            }
        }
    }

    client = Client()
    result = client.graphql(query)
    flow_run = result.data.flow_run

    if not flow_run:
        click.echo("Flow run {} not found".format(flow_run_id))
        raise ValueError("Flow run {} not found".format(flow_run_id))

    # Set the `running_with_backend` context variable to enable logging
    with merak.context(running_with_backend=True):
        try:
            flow_data = flow_run[0].flow
            storage_schema = merak.serialization.storage.StorageSchema()
            storage = storage_schema.load(flow_data.storage)

            # populate global secrets
            secrets = merak.context.get("secrets", {})
            for secret in storage.secrets:
                secrets[secret] = merakSecret(name=secret).run()

            with merak.context(secrets=secrets, loading_flow=True):
                flow = storage.get_flow(flow_data.name)

            with merak.context(secrets=secrets):
                runner_cls = get_default_flow_runner_class()
                runner_cls(flow=flow).run()

        except Exception as exc:
            msg = "Failed to load and execute flow run: {}".format(repr(exc))
            state = merak.engine.state.Failed(message=msg)
            client.set_flow_run_state(flow_run_id=flow_run_id, state=state)
            client.write_run_logs(
                dict(
                    flow_run_id=flow_run_id,  # type: ignore
                    name="execute flow-run",
                    message=msg,
                    level="ERROR",
                )
            )
            click.echo(str(exc))
            raise exc
