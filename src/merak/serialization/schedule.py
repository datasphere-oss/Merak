from typing import Any

from dateutil import rrule
from marshmallow import fields, post_dump, post_load
import pendulum
import merak
from merak.utilities.serialization import (
    DateTimeTZ,
    JSONCompatible,
    ObjectSchema,
    OneOfSchema,
    StatefulFunctionReference,
)

FILTERS = [
    merak.schedules.filters.on_datetime,
    merak.schedules.filters.on_date,
    merak.schedules.filters.at_time,
    merak.schedules.filters.between_datetimes,
    merak.schedules.filters.between_dates,
    merak.schedules.filters.between_times,
    merak.schedules.filters.is_weekday,
    merak.schedules.filters.is_weekend,
    merak.schedules.filters.is_month_end,
    merak.schedules.filters.is_day_of_week,
    merak.schedules.filters.is_month_start,
]

ADJUSTMENTS = [
    merak.schedules.adjustments.add,
    merak.schedules.adjustments.next_weekday,
]


class IntervalClockSchema(ObjectSchema):
    class Meta:
        object_class = merak.schedules.clocks.IntervalClock

    start_date = DateTimeTZ(allow_none=True)
    end_date = DateTimeTZ(allow_none=True)
    interval = fields.TimeDelta(precision="microseconds", required=True)
    parameter_defaults = fields.Dict(
        keys=fields.Str(), values=JSONCompatible(), allow_none=True
    )
    labels = fields.List(fields.Str(), allow_none=True)

    @post_dump
    def _interval_validation(self, data: dict, **kwargs: Any) -> dict:
        """
        Ensures interval is at least one minute in length
        """
        if data["interval"] / 1e6 < 60:
            raise ValueError(
                "Interval can not be less than one minute when deploying to merak Cloud."
            )
        return data

    @post_load
    def create_object(self, data: dict, **kwargs: Any):
        if data["interval"].total_seconds() < 60:
            raise ValueError(
                "Interval can not be less than one minute when deploying to merak Cloud."
            )
        base_obj = super().create_object(data)
        return base_obj


class CronClockSchema(ObjectSchema):
    class Meta:
        object_class = merak.schedules.clocks.CronClock

    start_date = DateTimeTZ(allow_none=True)
    end_date = DateTimeTZ(allow_none=True)
    cron = fields.String(required=True)
    parameter_defaults = fields.Dict(
        keys=fields.Str(), values=JSONCompatible(), allow_none=True
    )
    labels = fields.List(fields.Str(), allow_none=True)
    day_or = fields.Boolean(allow_none=True)


class DatesClockSchema(ObjectSchema):
    class Meta:
        object_class = merak.schedules.clocks.DatesClock

    dates = fields.List(DateTimeTZ(), required=True)
    parameter_defaults = fields.Dict(
        keys=fields.Str(), values=JSONCompatible(), allow_none=True
    )
    labels = fields.List(fields.Str(), allow_none=True)


class RRuleSchema(ObjectSchema):
    class Meta:
        object_class = rrule.rrule

    rr = fields.Method("dump_rrule_str", "load_rrule_str")
    # RRule str serialization does not record the timezone for some weird reason. So, in order to
    # get it back we also serialize the start/until params which may have TZ, and replace them
    _dtstart = fields.DateTime(
        allow_none=True, load_default=pendulum.datetime(2018, 6, 20)
    )
    _until = fields.DateTime(allow_none=True)

    def dump_rrule_str(self, obj):
        return str(obj)

    def load_rrule_str(self, value):
        return rrule.rrulestr(value)

    @post_load
    def create_object(self, data: dict, **kwargs: Any) -> Any:
        rr = data["rr"]
        params = {"dtstart": data["_dtstart"]}
        if "_until" in data:
            params["until"] = data["_until"]
        rr = rr.replace(**params)
        return rr


class RRuleSetSchema(ObjectSchema):
    class Meta:
        object_class = rrule.rruleset

    _rrule = fields.List(fields.Nested(RRuleSchema))
    _rdate = fields.List(fields.DateTime)
    _exrule = fields.List(fields.Nested(RRuleSchema))
    _exdate = fields.List(fields.DateTime)

    @post_load
    def create_object(self, data: dict, **kwargs: Any) -> Any:
        rrs = rrule.rruleset()
        for rr in data.get("_rrule", []):
            rrs.rrule(rr)
        for dt in data.get("_rdate", []):
            rrs.rdate(dt)
        for exrr in data.get("_exrule", []):
            rrs.exrule(exrr)
        for exdt in data.get("_exdate", []):
            rrs.exdate(exdt)
        return rrs


class RRuleBaseSchema(OneOfSchema):
    type_schemas = {"rrule": RRuleSchema, "rruleset": RRuleSetSchema}


class RRuleClockSchema(ObjectSchema):
    class Meta:
        object_class = merak.schedules.clocks.RRuleClock

    rrule_obj = fields.Nested(RRuleBaseSchema)
    start_date = DateTimeTZ(allow_none=True)
    end_date = DateTimeTZ(allow_none=True)
    parameter_defaults = fields.Dict(
        keys=fields.Str(), values=JSONCompatible(), allow_none=True
    )
    labels = fields.List(fields.Str(), allow_none=True)


class ClockSchema(OneOfSchema):
    """
    Field that chooses between several nested schemas
    """

    # map class name to schema
    type_schemas = {
        "IntervalClock": IntervalClockSchema,
        "CronClock": CronClockSchema,
        "DatesClock": DatesClockSchema,
        "RRuleClock": RRuleClockSchema,
    }


class ScheduleSchema(ObjectSchema):
    """
    The serialization schema for Schedule types
    """

    class Meta:
        object_class = merak.schedules.Schedule

    clocks = fields.Nested(ClockSchema, required=True, many=True)
    filters = fields.List(
        StatefulFunctionReference(
            valid_functions=FILTERS, reject_invalid=True, allow_none=True
        )
    )
    or_filters = fields.List(
        StatefulFunctionReference(
            valid_functions=FILTERS, reject_invalid=True, allow_none=True
        )
    )
    not_filters = fields.List(
        StatefulFunctionReference(
            valid_functions=FILTERS, reject_invalid=True, allow_none=True
        )
    )
    adjustments = fields.List(
        StatefulFunctionReference(
            valid_functions=ADJUSTMENTS, reject_invalid=True, allow_none=True
        )
    )
