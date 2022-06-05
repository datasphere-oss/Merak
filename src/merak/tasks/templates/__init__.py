from merak.tasks.templates.strings import StringFormatter

try:
    from merak.tasks.templates.jinja2 import JinjaTemplate
except ImportError:
    pass

__all__ = ["JinjaTemplate", "StringFormatter"]
