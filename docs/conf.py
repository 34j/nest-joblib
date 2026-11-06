"""Sphinx configuration for the nest-joblib documentation."""

from pathlib import Path
from typing import Any

from sphinx.application import Sphinx
from sphinx.ext import apidoc

project = "nest-joblib"
copyright = "2023, 34j"  # noqa: A001
author = "34j"
release = "0.1.7"

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]
napoleon_google_docstring = False
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
exclude_patterns = ["_build"]
html_theme = "furo"


def generate_documentation_sources(app: Sphinx, _: Any) -> None:
    """Create the documentation index and changelog for this build."""
    docs_path = Path(app.srcdir)
    (docs_path / "index.md").write_text(
        """```{include} ../README.md

```

```{toctree}
:maxdepth: 2
:glob:

*
```
""",
        encoding="utf-8",
    )
    (docs_path / "changelog.md").write_text("```{include} ../CHANGELOG.md\n```\n", encoding="utf-8")


def run_apidoc(_: Any) -> None:
    """Generate the API reference source document."""
    docs_path = Path(__file__).parent
    module_path = docs_path.parent / "src" / "nest_joblib"
    apidoc.main([
        "--force",
        "--module-first",
        "--no-toc",
        "-o",
        docs_path.as_posix(),
        module_path.as_posix(),
    ])


def cleanup_generated_sources(app: Sphinx, _exception: Exception | None) -> None:
    """Remove documentation sources generated only for the build."""
    docs_path = Path(app.srcdir)
    for name in ("index.md", "changelog.md", "nest_joblib.rst"):
        (docs_path / name).unlink(missing_ok=True)


def setup(app: Sphinx) -> None:
    """Register the documentation generation hooks."""
    app.connect("config-inited", generate_documentation_sources)
    app.connect("builder-inited", run_apidoc)
    app.connect("build-finished", cleanup_generated_sources)
