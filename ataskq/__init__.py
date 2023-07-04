try:
    from .version import __version__, __build__
except:
    __version__ = "0.0.0"
    __build__ = "dev"


from .ataskq import TaskQ, targs
from .task import Task