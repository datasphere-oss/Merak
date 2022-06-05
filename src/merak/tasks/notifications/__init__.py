"""
Collection of tasks for sending notifications.

Useful for situations in which state handlers are inappropriate.
"""
from merak.tasks.notifications.email_task import EmailTask
from merak.tasks.notifications.slack_task import SlackTask
from merak.tasks.notifications.pushbullet_task import PushbulletTask

__all__ = ["EmailTask", "PushbulletTask", "SlackTask"]
