from typing import Any

import merak
from merak import Task
from merak.client import Client
from merak.utilities.tasks import defaults_from_attrs


class RenameFlowRun(Task):
    """
    Task used to rename a running flow.

    Args:
        - flow_run_id (str, optional): The ID of the flow run to rename.
        - flow_run_name (str, optional): The new flow run name.
        - **kwargs (dict, optional): additional keyword arguments to pass to the Task constructor
    """

    def __init__(
        self,
        flow_run_id: str = None,
        flow_run_name: str = None,
        **kwargs: Any,
    ):
        self.flow_run_id = flow_run_id
        self.flow_run_name = flow_run_name
        super().__init__(**kwargs)

    @defaults_from_attrs("flow_run_id", "flow_run_name")
    def run(self, flow_run_id: str, flow_run_name: str) -> bool:
        """
        Args:
            - flow_run_id (str, optional): The ID of the flow run to rename. If `None`,
                the `flow_run_id` from `merak.context` will be used as default value
            - flow_run_name (str, optional): The new flow run name

        Returns:
            - bool: Boolean representing whether the flow run was renamed successfully or not.

        Raises:
            - ValueError: If `flow_run_id` is not provided and `flow_run_id` does not exist
                in `merak.context`
            - ValueError: If `flow_run_name` is not provided

        Example:
            ```python
            from merak.tasks.merak.flow_rename import FlowRenameTask

            rename_flow = FlowRenameTask(flow_name="A new flow run name")
            ```
        """
        flow_run_id = flow_run_id or merak.context.get("flow_run_id")
        if not flow_run_id:
            raise ValueError(
                "`flow_run_id` must be explicitly provided or available in the context"
            )
        if flow_run_name is None:
            raise ValueError("Must provide a flow name.")

        client = Client()
        return client.set_flow_run_name(flow_run_id, flow_run_name)
