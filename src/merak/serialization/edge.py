from marshmallow import fields

import merak
from merak.serialization.task import TaskSchema
from merak.utilities.serialization import ObjectSchema


class EdgeSchema(ObjectSchema):
    class Meta:
        object_class = lambda: merak.core.Edge

    upstream_task = fields.Nested(TaskSchema, only=["slug"])
    downstream_task = fields.Nested(TaskSchema, only=["slug"])
    key = fields.String(allow_none=True)
    mapped = fields.Boolean(allow_none=True)
    flattened = fields.Boolean(allow_none=True)
