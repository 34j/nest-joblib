# nest-joblib

<p align="center">
  <a href="https://github.com/34j/nest-joblib/actions/workflows/ci.yml?query=branch%3Amain">
    <img src="https://img.shields.io/github/actions/workflow/status/34j/nest-joblib/ci.yml?branch=main&label=CI&logo=github&style=flat-square" alt="CI Status" >
  </a>
  <a href="https://nest-joblib.readthedocs.io">
    <img src="https://img.shields.io/readthedocs/nest-joblib.svg?logo=read-the-docs&logoColor=fff&style=flat-square" alt="Documentation Status">
  </a>
</p>
<p align="center">
  <a href="https://codecov.io/gh/34j/nest-joblib">
    <img src="https://img.shields.io/codecov/c/github/34j/nest-joblib.svg?logo=codecov&logoColor=fff&style=flat-square" alt="Test coverage percentage">
  </a>
  <a href="https://github.com/astral-sh/uv">
    <img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json" alt="uv">
  </a>
  <a href="https://github.com/astral-sh/ruff">
    <img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json" alt="Ruff">
  </a>
  <a href="https://github.com/j178/prek">
    <img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/j178/prek/master/docs/assets/badge-v0.json" alt="prek">
  </a>
</p>
<p align="center">
  <a href="https://pypi.org/project/nest-joblib/">
    <img src="https://img.shields.io/pypi/v/nest-joblib.svg?logo=python&logoColor=fff&style=flat-square" alt="PyPI Version">
  </a>
  <img src="https://img.shields.io/pypi/pyversions/nest-joblib.svg?style=flat-square&logo=python&amp;logoColor=fff" alt="Supported Python versions">
  <img src="https://img.shields.io/pypi/l/nest-joblib.svg?style=flat-square" alt="License">
</p>

---

**Documentation**: <a href="https://nest-joblib.readthedocs.io" target="_blank">https://nest-joblib.readthedocs.io </a>

**Source Code**: <a href="https://github.com/34j/nest-joblib" target="_blank">https://github.com/34j/nest-joblib </a>

---

Patch joblib to allow nested parallelism.

## Installation

Install this via pip (or your favourite package manager):

```shell
pip install nest-joblib
```

## Usage

```python
from nest_joblib import apply

apply()
```

With the above code, LokyBackend supports nested-parallelism.

## Advanced Usage

The following joblib specification of not doing nested-parallelism may be inefficient in an environment with sufficient memory.

`joblib/_parallel_backends.py`:

```python
    def get_nested_backend(self):
        """Backend instance to be used by nested Parallel calls.

        By default a thread-based backend is used for the first level of
        nesting. Beyond, switch to sequential backend to avoid spawning too
        many threads on the host.
        """
        nesting_level = getattr(self, 'nesting_level', 0) + 1
        if nesting_level > 1:
            return SequentialBackend(nesting_level=nesting_level), None
        else:
            return ThreadingBackend(nesting_level=nesting_level), None
```

After calling `nest_joblib.apply()`, when `joblib.parallel.register_parallel_backend(name, backend)` is called, a subclass of `backend` with modified `get_nested_backend` is dynamically generated and registered with the name `f"nested-{name}"`.

```python
from joblib.parallel import parallel_backend
from nest_joblib import apply
from ray.util.joblib import register_ray

# use LokyBackend
apply()

# use DaskDistributedBackend
apply()
parallel_backend("nested-dask")

# use RayBackend
apply()
register_ray()
parallel_backend("nested-ray")

# use custom backend
from joblib.parallel import LokyBackend
apply()
class MyBackend(LokyBackend):
    pass
register_parallel_backend("custom", MyBackend)
parallel_backend("nested-custom")
```

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- prettier-ignore-start -->
<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/34j"><img src="https://avatars.githubusercontent.com/u/55338215?v=4?s=80" width="80px;" alt="34j"/><br /><sub><b>34j</b></sub></a><br /><a href="https://github.com/34j/nest-joblib/commits?author=34j" title="Code">💻</a> <a href="#ideas-34j" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/34j/nest-joblib/commits?author=34j" title="Documentation">📖</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
<!-- prettier-ignore-end -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!
