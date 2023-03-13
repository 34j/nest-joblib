from __future__ import annotations

import re
import sys
from copy import deepcopy
from typing import Any, Dict, Type

import joblib.parallel
from joblib.parallel import (
    ParallelBackendBase,
    parallel_backend,
    register_parallel_backend,
)

if sys.version_info >= (3, 11):
    from typing import Self  # nocover
else:
    from typing_extensions import Self  # nocover


class NestedBackendMixin:
    def get_nested_backend(self, *args: Any, **kwargs: Any) -> tuple[Self, None]:
        return self.__class__(), None


def _create_nested_backend(
    backend_class: type[ParallelBackendBase],
) -> type[ParallelBackendBase]:
    if issubclass(backend_class, NestedBackendMixin):
        return backend_class
    return type(
        f"Nested{backend_class.__name__}", (NestedBackendMixin, backend_class), {}
    )


class _NestedBackendDict(Dict[str, Type[ParallelBackendBase]]):
    def __setitem__(self, __key: str, __value: type[ParallelBackendBase]) -> None:
        if isinstance(__key, str) and re.match("nested-", __key) is None:
            super().__setitem__(f"nested-{__key}", _create_nested_backend(__value))
        return super().__setitem__(__key, __value)


def apply(*, set_default: bool = True, auto_register: bool = True) -> None:
    # BACKENDS
    for name, backend in deepcopy(joblib.parallel.BACKENDS).items():
        if re.match("nested-", name) is not None:
            continue
        register_parallel_backend(f"nested-{name}", _create_nested_backend(backend))

    # change the type of BACKENDS from dict to _NestedBackendDict
    if auto_register:
        joblib.parallel.BACKENDS = _NestedBackendDict(joblib.parallel.BACKENDS)

    # EXTERNAL_BACKENDS
    for name, register_backend in deepcopy(joblib.parallel.EXTERNAL_BACKENDS).items():
        if re.match("nested-", name) is not None:
            continue
        joblib.parallel.EXTERNAL_BACKENDS[f"nested-{name}"] = register_backend

    # set DEFAULT_BACKEND to nested-loky
    if set_default:
        parallel_backend("nested-loky")
