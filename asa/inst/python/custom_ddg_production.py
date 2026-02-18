"""Backward-compatible import shim for the ASA backend API.

Legacy code can still import ``custom_ddg_production`` while runtime callers
should migrate to ``asa_backend.agent_api``.
"""

from asa_backend import agent_api as _api


def __getattr__(name):
    return getattr(_api, name)


def __dir__():
    return sorted(set(globals()) | set(dir(_api)))


__all__ = [name for name in dir(_api) if not name.startswith("__")]
