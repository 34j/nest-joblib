from unittest import TestCase

from joblib import delayed
from joblib.parallel import Parallel, get_active_backend, parallel_backend

from nest_joblib import apply


def _nested(level: int, max_level: int) -> list[str]:
    backend, n_jobs = get_active_backend()
    if level < max_level:
        parallel_result = Parallel(n_jobs=-1, verbose=1)(
            delayed(_nested)(level + 1, max_level) for _ in range(n_jobs)
        )
        assert parallel_result is not None
        return [x for y in parallel_result for x in y] + [backend.__class__.__name__]
    else:
        return [backend.__class__.__name__]


class TestMainNoAutoRegister(TestCase):
    def setUp(self) -> None:
        apply(auto_register=False)

    def test_nested_ray(self):
        from ray.util.joblib import register_ray

        register_ray()
        with self.assertRaises(ValueError):
            with parallel_backend("nested-ray"):
                pass


class TestMain(TestCase):
    def setUp(self) -> None:
        apply(set_default=False)

    def test_nested_loky(self):
        result = _nested(0, 3)
        for backend in result:
            self.assertEqual(backend, "NestedLokyBackend")

    def test_nested_loky_default(self):
        apply(set_default=True)
        result = _nested(0, 3)
        for backend in result:
            self.assertEqual(backend, "NestedLokyBackend")

    def test_nested_ray(self):
        from ray.util.joblib import register_ray

        register_ray()
        with parallel_backend("nested-ray"):
            result = _nested(0, 3)
        for backend in result:
            self.assertEqual(backend, "NestedRayBackend")

    def test_nested_dask(self):
        from dask.distributed import Client

        _ = Client(processes=False)
        with parallel_backend("nested-dask"):
            result = _nested(0, 3)
        for backend in result:
            self.assertEqual(backend, "NestedDaskDistributedBackend")
