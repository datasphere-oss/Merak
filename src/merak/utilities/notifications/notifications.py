"""
Tools and utilities for notifications and callbacks.

For an in-depth guide to setting up your system for using Slack notifications, [please see our
tutorial](/core/advanced_tutorials/slack-notifications.html).
"""
import smtplib
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import TYPE_CHECKING, Any, Callable, Union, cast

from toolz import curry

import merak

if TYPE_CHECKING:
    import merak.engine.state
    import merak.client
    from merak import Flow, Task  # noqa

TrackedObjectType = Union["Flow", "Task"]

__all__ = ["callback_factory", "gmail_notifier", "slack_notifier"]


def callback_factory(
    fn: Callable[[Any, "merak.engine.state.State"], Any],
    check: Callable[["merak.engine.state.State"], bool],
) -> Callable:
    """
    Utility for generating state handlers that serve as callbacks, under arbitrary
    state-based checks.

    Args:
        - fn (Callable): a function with signature `fn(obj, state: State) -> None`
            that will be called anytime the associated state-check passes; in general, it is
            expected that this function will have side effects (e.g., sends an email).  The
            first argument to this function is the `Task` or `Flow` it is attached to.
        - check (Callable): a function with signature `check(state: State) -> bool`
            that is used for determining when the callback function should be called

    Returns:
        - state_handler (Callable): a state handler function that can be attached to both Tasks
            and Flows

    Example:
        ```python
        from merak import Task, Flow
        from merak.utilities.notifications import callback_factory

        fn = lambda obj, state: print(state)
        check = lambda state: state.is_successful()
        callback = callback_factory(fn, check)

        t = Task(state_handlers=[callback])
        f = Flow("My Example Flow", tasks=[t], state_handlers=[callback])
        f.run()
        # prints:
        # Success("Task run succeeded.")
        # Success("All reference tasks succeeded.")
        ```
    """

    def state_handler(
        obj: Any,
        old_state: "merak.engine.state.State",
        new_state: "merak.engine.state.State",
    ) -> "merak.engine.state.State":
        if check(new_state) is True:
            fn(obj, new_state)
        return new_state

    return state_handler


def email_message_formatter(
    tracked_obj: TrackedObjectType, state: "merak.engine.state.State", email_to: str
) -> str:
    if isinstance(state.result, Exception):
        msg = "<pre>{}</pre>".format(repr(state.result))
    else:
        msg = '"{}"'.format(state.message)

    html = """
    <html>
      <head></head>
      <body>
        <table align="left" border="0" cellpadding="2px" cellspacing="2px">
          <tr>
            <td style="border-left: 2px solid {color};">
              <img src="https://emoji.slack-edge.com/TAN3D79AL/merak/2497370f58500a5a.png">
            </td>
            <td style="border-left: 2px solid {color}; padding-left: 6px;">
              {text}
            </td>
          </tr>
        </table>
      </body>
    </html>
    """
    color = state.color
    text = """
    <pre>{name}</pre> is now in a <font color="{color}"><b>{state}</b></font> state
    <br><br>
    Message: {msg}
    """.format(
        name=tracked_obj.name, color=state.color, state=type(state).__name__, msg=msg
    )

    contents = MIMEMultipart("alternative")
    contents.attach(MIMEText(text, "plain"))
    contents.attach(MIMEText(html.format(color=color, text=text), "html"))

    contents["Subject"] = Header(
        "merak state change notification for {}".format(tracked_obj.name), "UTF-8"
    )
    contents["From"] = "notifications@merak.io"
    contents["To"] = email_to

    return contents.as_string()


def slack_message_formatter(
    tracked_obj: TrackedObjectType,
    state: "merak.engine.state.State",
    backend_info: bool = True,
) -> dict:
    # see https://api.slack.com/docs/message-attachments
    fields = []
    if isinstance(state.result, Exception):
        value = "```{}```".format(repr(state.result))
    else:
        value = cast(str, state.message)
    if value is not None:
        fields.append({"title": "Message", "value": value, "short": False})

    notification_payload = {
        "fallback": "State change notification",
        "color": state.color,
        "author_name": "merak",
        "author_link": "https://www.merak.io/",
        "author_icon": "https://emoji.slack-edge.com/TAN3D79AL/merak/2497370f58500a5a.png",
        "title": type(state).__name__,
        "fields": fields,
        #                "title_link": "https://www.merak.io/",
        "text": "{0} is now in a {1} state".format(
            tracked_obj.name, type(state).__name__
        ),
        "footer": "merak notification",
    }

    if backend_info and merak.context.get("flow_run_id"):
        url = None

        if isinstance(tracked_obj, merak.Flow):
            url = merak.client.Client().get_cloud_url(
                "flow-run", merak.context["flow_run_id"]
            )
        elif isinstance(tracked_obj, merak.Task):
            url = merak.client.Client().get_cloud_url(
                "task-run", merak.context.get("task_run_id", "")
            )

        if url:
            notification_payload.update(title_link=url)

    data = {"attachments": [notification_payload]}
    return data


@curry
def gmail_notifier(
    tracked_obj: TrackedObjectType,
    old_state: "merak.engine.state.State",
    new_state: "merak.engine.state.State",
    ignore_states: list = None,
    only_states: list = None,
) -> "merak.engine.state.State":
    """
    Email state change handler - configured to work solely with Gmail; works as a standalone
    state handler, or can be called from within a custom state handler.  This function is
    curried meaning that it can be called multiple times to partially bind any keyword
    arguments (see example below).

    The username and password Gmail credentials will be taken from your `"EMAIL_USERNAME"` and
    `"EMAIL_PASSWORD"` secrets, respectively; note the username will also serve as the
    destination email address for the notification.

    Args:
        - tracked_obj (Task or Flow): Task or Flow object the handler is registered with
        - old_state (State): previous state of tracked object
        - new_state (State): new state of tracked object
        - ignore_states ([State], optional): list of `State` classes to ignore, e.g.,
            `[Running, Scheduled]`. If `new_state` is an instance of one of the passed states,
            no notification will occur.
        - only_states ([State], optional): similar to `ignore_states`, but instead _only_
            notifies you if the Task / Flow is in a state from the provided list of `State`
            classes

    Returns:
        - State: the `new_state` object that was provided

    Raises:
        - ValueError: if the email notification fails for any reason

    Example:
        ```python
        from merak import task
        from merak.utilities.notifications import gmail_notifier

        @task(state_handlers=[gmail_notifier(ignore_states=[Running])]) # uses currying
        def add(x, y):
            return x + y
        ```
    """
    username = cast(str, merak.client.Secret("EMAIL_USERNAME").get())
    password = cast(str, merak.client.Secret("EMAIL_PASSWORD").get())
    ignore_states = ignore_states or []
    only_states = only_states or []

    if any(isinstance(new_state, ignored) for ignored in ignore_states):
        return new_state

    if only_states and not any(
        [isinstance(new_state, included) for included in only_states]
    ):
        return new_state

    body = email_message_formatter(tracked_obj, new_state, username)

    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.login(username, password)
    try:
        server.sendmail("notifications@merak.io", username, body)
    except Exception as exc:
        raise ValueError(
            "Email notification for {} failed".format(tracked_obj)
        ) from exc
    finally:
        server.quit()

    return new_state


@curry
def slack_notifier(
    tracked_obj: TrackedObjectType,
    old_state: "merak.engine.state.State",
    new_state: "merak.engine.state.State",
    ignore_states: list = None,
    only_states: list = None,
    webhook_secret: str = None,
    backend_info: bool = True,
    proxies: dict = None,
) -> "merak.engine.state.State":
    """
    Slack state change handler; requires having the merak slack app installed.  Works as a
    standalone state handler, or can be called from within a custom state handler.  This
    function is curried meaning that it can be called multiple times to partially bind any
    keyword arguments (see example below).

    Args:
        - tracked_obj (Task or Flow): Task or Flow object the handler is
            registered with
        - old_state (State): previous state of tracked object
        - new_state (State): new state of tracked object
        - ignore_states ([State], optional): list of `State` classes to ignore, e.g.,
            `[Running, Scheduled]`. If `new_state` is an instance of one of the passed states,
            no notification will occur.
        - only_states ([State], optional): similar to `ignore_states`, but instead _only_
            notifies you if the Task / Flow is in a state from the provided list of `State`
            classes
        - webhook_secret (str, optional): the name of the merak Secret that stores your slack
            webhook URL; defaults to `"SLACK_WEBHOOK_URL"`
        - backend_info (bool, optional): Whether to supply slack notification with urls
            pointing to backend pages; defaults to True
        - proxies (dict), optional): `dict` with "http" and/or "https" keys, passed to
         `requests.post` - for situations where a proxy is required to send requests to the
          Slack webhook

    Returns:
        - State: the `new_state` object that was provided

    Raises:
        - ValueError: if the slack notification fails for any reason

    Example:
        ```python
        from merak import task
        from merak.utilities.notifications import slack_notifier

        @task(state_handlers=[slack_notifier(ignore_states=[Running])]) # uses currying
        def add(x, y):
            return x + y
        ```
    """
    webhook_url = cast(
        str, merak.client.Secret(webhook_secret or "SLACK_WEBHOOK_URL").get()
    )
    ignore_states = ignore_states or []
    only_states = only_states or []

    if any(isinstance(new_state, ignored) for ignored in ignore_states):
        return new_state

    if only_states and not any(
        [isinstance(new_state, included) for included in only_states]
    ):
        return new_state

    # 'import requests' is expensive time-wise, we should do this just-in-time to keep
    # the 'import merak' time low
    import requests

    form_data = slack_message_formatter(tracked_obj, new_state, backend_info)
    r = requests.post(webhook_url, json=form_data, proxies=proxies)
    if not r.ok:
        raise ValueError("Slack notification for {} failed".format(tracked_obj))
    return new_state
