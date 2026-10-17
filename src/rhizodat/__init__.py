"""rhizodat: a bitemporal polyglot data layer.

The specification v0.1 is built against lives in the documentation (docs/).
"""

from importlib.metadata import version

from rhizodat.api import Rhizodat
from rhizodat.core.adapter import Adapter, Scheme, register_adapter
from rhizodat.core.coordinator import Aborted, Committed, CommittedPending, InDoubt, StoreFull
from rhizodat.core.errors import ErrorKind
from rhizodat.core.mapping import Ref, entity
from rhizodat.core.retry import Retry
from rhizodat.core.unit_of_work import UnitOfWork
from rhizodat.core.vectors import Match, Vector

__version__ = version("rhizodat")

__all__ = [
    "Aborted",
    "Adapter",
    "Committed",
    "CommittedPending",
    "ErrorKind",
    "InDoubt",
    "Match",
    "Ref",
    "Retry",
    "Rhizodat",
    "Scheme",
    "StoreFull",
    "UnitOfWork",
    "Vector",
    "__version__",
    "entity",
    "register_adapter",
]
