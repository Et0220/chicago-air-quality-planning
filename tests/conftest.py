"""Optional local Spark integration requires a working Java 17 runtime."""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


def java17_home():
    explicit = os.environ.get("JAVA_HOME")
    candidates = [str(Path(explicit) / "bin/java")] if explicit else []
    if not explicit:
        if command := shutil.which("java"):
            candidates.append(command)
        if sys.platform == "darwin":
            try:
                found = subprocess.run(
                    ["/usr/libexec/java_home", "-v", "17"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=True,
                )
                candidates.insert(0, str(Path(found.stdout.strip()) / "bin/java"))
            except OSError, subprocess.SubprocessError:
                pass
    for command in candidates:
        try:
            result = subprocess.run(
                [command, "-version"], capture_output=True, text=True, timeout=10
            )
        except OSError, subprocess.SubprocessError:
            continue
        if result.returncode == 0 and re.search(
            r'version "17(?:\.|\")', result.stdout + result.stderr
        ):
            return str(Path(command).resolve().parent.parent)
    return None


@pytest.fixture(scope="module")
def spark_session():
    pytest.importorskip("pyspark")
    home = java17_home()
    if home is None:
        pytest.skip("Spark tests require Java 17; install it and set JAVA_HOME")

    from pyspark.sql import SparkSession

    with pytest.MonkeyPatch.context() as env:
        env.setenv("JAVA_HOME", home)
        env.setenv("PYSPARK_PYTHON", sys.executable)
        if "SPARK_LOCAL_IP" not in os.environ:
            env.setenv("SPARK_LOCAL_IP", "127.0.0.1")
        session = (
            SparkSession.builder.master("local[2]")
            .appName("epa-normalization-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
        try:
            yield session
        finally:
            session.stop()
