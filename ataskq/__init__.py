try:
    from .version import __version__, __build__
except Exception:
    __version__ = "0.0.0"
    __build__ = "dev"

__schema_version__ = 4

from .ataskq import TaskQ, targs
from .models import Job, StateKWArg, Task, EStatus
