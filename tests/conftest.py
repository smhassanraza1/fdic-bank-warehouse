"""Shared pytest fixtures.

`spark` is session-scoped and built lazily inside the fixture function (not imported at module
level) so that plain `pytest` runs — including the existing non-Spark suite — still collect and
pass in an environment without the `spark` dependency group or a JDK installed. It's only
constructed when a test actually requests the fixture.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("fig-cleaning-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
