try:
    from .version import __version__, __build__
except:
    __version__ = "0.0.0"
    __build__ = "dev"

__schema_version__ = 1

from .ataskq import TaskQ, targs
from .task import Task, EStatus