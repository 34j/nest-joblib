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
    """Mixin class for nested parallel backend.
    Overrides `ParallelBackendBase.get_nested_backend` method
    to return Self class.
    """

    def get_nested_backend(self, *args: Any, **kwargs: Any) -> tuple[Self, None]:
        """Get nested parallel backend.

        Returns
        -------
        tuple[Self, None]

        """
        return self.__class__(), None


def _create_nested_backend(
    backend_class: type[ParallelBackendBase],
) -> type[ParallelBackendBase]:
    """Dynamically create nested parallel backend class.

    Parameters
    ----------
    backend_class : type[ParallelBackendBase]
        Base class of parallel backend.

    Returns
    -------
    type[ParallelBackendBase]
        Nested parallel backend class.
    """
    if issubclass(backend_class, NestedBackendMixin):
        return backend_class
    return type(
        f"Nested{backend_class.__name__}", (NestedBackendMixin, backend_class), {}
    )


class _NestedBackendDict(Dict[str, Type[ParallelBackendBase]]):
    """Dict class that dynamically creates and registers
    nested parallel backend class with key name "nested-{key}"
    if key name is not "nested-{key}".
    """

    def __setitem__(self, __key: str, __value: type[ParallelBackendBase]) -> None:
        """Set self[__key] to __value.
        If __key is not "nested-{key}",
        dynamically create nested parallel backend class
        and register it with key name "nested-{key}".
        """
        if isinstance(__key, str) and re.match("nested-", __key) is None:
            super().__setitem__(f"nested-{__key}", _create_nested_backend(__value))
        return super().__setitem__(__key, __value)


def apply(*, set_default: bool = True, auto_register: bool = True) -> None:
    '''Apply `nest_joblib`.

    The following joblib specification of not doing nested-parallelism may
    be inefficient in an environment with sufficient memory.

    `joblib/_parallel_backends.py`:

    >>> def get_nested_backend(self):
    >>> """Backend instance to be used by nested Parallel calls.
    >>>
    >>> By default a thread-based backend is used for the first level of
    >>> nesting. Beyond, switch to sequential backend to avoid spawning too
    >>> many threads on the host.
    >>> """
    >>> nesting_level = getattr(self, 'nesting_level', 0) + 1
    >>> if nesting_level > 1:
    >>>     return SequentialBackend(nesting_level=nesting_level), None
    >>> else:
    >>>     return ThreadingBackend(nesting_level=nesting_level), None

    After calling this function,
    when joblib.parallel.register_parallel_backend(name, backend) is called,
    a subclass of backend with modified get_nested_backend is
    dynamically generated and registered with the name f"nested-{name}".

    Parameters
    ----------
    set_default : bool, optional
        Whether to set DEFAULT_BACKEND to nested-loky, by default True
    auto_register : bool, optional
        Whether to automatically register nested parallel backend.
        Useful when registering external parallel backend like `ray`
        AFTER calling this function.
        If False, nested version of already registered parallel backends
        will still be registered.
        If error occurs, setting this to False may help, by default True

    Examples
    --------
    Using `nested-loky` (nested version of `loky`) as default parallel backend:
    >>> import nest_joblib
    >>> nest_joblib.apply()

    Using `nested-dask` (nested version of another parallel backend `dask`)
    as default parallel backend:
    >>> from joblib import parallel_backend
    >>> from nest_joblib import apply
    >>>
    >>> apply()
    >>> parallel_backend("nested-dask")

    Using `nested-ray` (nested version of external parallel backend `ray`)
    as default parallel backend:
    >>> from joblib import parallel_backend
    >>> from nest_joblib import apply
    >>> from ray.util.joblib import register_ray
    >>>
    >>> apply()
    >>> register_ray()
    >>> parallel_backend("nested-ray")
    '''
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
