"""datorium: a bitemporal polyglot data layer.

The specification v0.1 is built against lives in the repository README.
"""

from importlib.metadata import version

from datorium.api import Datorium
from datorium.core.adapter import Adapter, Scheme, register_adapter
from datorium.core.coordinator import Aborted, Committed, CommittedPending, InDoubt, StoreFull
from datorium.core.errors import ErrorKind
from datorium.core.mapping import Ref, entity
from datorium.core.retry import Retry
from datorium.core.unit_of_work import UnitOfWork

__version__ = version("datorium")

__all__ = [
    "Aborted",
    "Adapter",
    "Committed",
    "CommittedPending",
    "Datorium",
    "ErrorKind",
    "InDoubt",
    "Ref",
    "Retry",
    "Scheme",
    "StoreFull",
    "UnitOfWork",
    "__version__",
    "entity",
    "register_adapter",
]
