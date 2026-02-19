"""Tor configuration."""

from __future__ import annotations

from ._legacy_transport import configure_tor, configure_tor_registry

__all__ = [
    "configure_tor",
    "configure_tor_registry",
]
