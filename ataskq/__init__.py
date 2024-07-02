try:
    from .version import __version__, __build__
except Exception:
    __version__ = "0.0.0"
    __build__ = "dev"

__schema_version__ = 6

from .taskq import TaskQ
from .models import Job, Task, EStatus, Object
