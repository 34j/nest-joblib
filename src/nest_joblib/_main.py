import functools
import re
import sys
from copy import deepcopy
from typing import Any, Type

from joblib.parallel import (
    BACKENDS,
    EXTERNAL_BACKENDS,
    ParallelBackendBase,
    parallel_backend,
    register_parallel_backend,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class NestedBackendMixin:
    def get_nested_backend(self, *args: Any, **kwargs: Any) -> tuple[Self, None]:
        return self.__class__(), None


def _create_nested_backend(
    backend_class: Type[ParallelBackendBase],
) -> Type[ParallelBackendBase]:
    return type(
        f"Nested{backend_class.__name__}", (NestedBackendMixin, backend_class), {}
    )


def apply(set_default: bool = True) -> None:
    for name, backend in deepcopy(BACKENDS).items():
        if re.match("nested-", name) is None:
            register_parallel_backend(f"nested-{name}", _create_nested_backend(backend))
    for name, register_backend in deepcopy(EXTERNAL_BACKENDS).items():

        @functools.wraps(register_backend)
        def register_nested_backend(*args: Any, **kwargs: Any) -> None:
            register_backend(*args, **kwargs)
            register_parallel_backend(
                f"nested-{name}", _create_nested_backend(BACKENDS[name])
            )

        EXTERNAL_BACKENDS[f"nested-{name}"] = register_nested_backend

    if set_default:
        parallel_backend("nested-loky")
