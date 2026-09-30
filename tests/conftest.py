"""Test setup: lightweight stand-ins for Airflow and the Snowflake connector.

The DAG module imports Airflow and snowflake.connector at the top. Neither is
needed to test its logic, and installing Airflow in CI is slow, so minimal
stubs are registered when the real packages are not importable. Tests patch
Variable, boto3 and snowflake.connector.connect themselves.
"""
import sys
import types


def _module(name, **attrs):
    mod = types.ModuleType(name)
    mod.__dict__.update(attrs)
    sys.modules[name] = mod
    return mod


def _install_airflow_stub():
    try:
        import airflow  # noqa: F401
        return
    except ImportError:
        pass

    class DAG:
        def __init__(self, *args, **kwargs):
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class Dataset:
        def __init__(self, uri):
            self.uri = uri

    class AirflowSkipException(Exception):
        pass

    class Variable:
        @staticmethod
        def get(key, default_var=None, deserialize_json=False):
            return default_var

        @staticmethod
        def set(key, value, serialize_json=False):
            raise AssertionError("Variable.set must be patched in tests")

    class _Operator:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def __rshift__(self, other):
            return other

    _module("airflow", DAG=DAG)
    _module("airflow.datasets", Dataset=Dataset)
    _module("airflow.exceptions", AirflowSkipException=AirflowSkipException)
    _module("airflow.models", Variable=Variable)
    _module("airflow.operators")
    _module("airflow.operators.python", PythonOperator=_Operator)


def _install_snowflake_stub():
    try:
        import snowflake.connector  # noqa: F401
        return
    except ImportError:
        pass

    def connect(**kwargs):
        raise AssertionError("snowflake.connector.connect must be patched in tests")

    connector = _module("snowflake.connector", connect=connect)
    _module("snowflake", connector=connector)


_install_airflow_stub()
_install_snowflake_stub()
