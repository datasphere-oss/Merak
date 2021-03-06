from typing import Any

import merak
from merak import Task
from merak.client import Client
from merak.utilities.tasks import defaults_from_attrs


class CancelFlowRun(Task):
    """
    Task to cancel a flow run. If `flow_run_id` is not provided,
    `flow_run_id` from `merak.context` will be used by default

    Args:
        - flow_run_id (str, optional): The ID of the flow run to cancel
        - **kwargs (dict, optional): additional keyword arguments to pass to the Task constructor
    """

    def __init__(
        self,
        flow_run_id: str = None,
        **kwargs: Any,
    ):
        self.flow_run_id = flow_run_id
        super().__init__(**kwargs)

    @defaults_from_attrs("flow_run_id")
    def run(self, flow_run_id: str = None) -> bool:
        """
        Args:
            - flow_run_id (str, optional): The ID of the flow run to cancel

        Returns:
            - bool: Whether the flow run was canceled successfully or not
        """
        flow_run_id = flow_run_id or merak.context.get("flow_run_id")
        if not flow_run_id:
            raise ValueError("Can't cancel a flow run without flow run ID.")

        client = Client()
        return client.cancel_flow_run(flow_run_id)
