"""Tor registry, circuit rotation, and blocklist helpers."""

from .search_transport import (
    _cleanup_expired_blocks,
    _cleanup_expired_proxy_blocks_db,
    _get_blocklist_status,
    _get_exit_ip,
    _mark_exit_bad,
    _mark_exit_good,
    _record_captcha_hit,
    _rotate_tor_circuit,
    configure_tor,
    configure_tor_registry,
)

__all__ = [
    "configure_tor",
    "configure_tor_registry",
    "_rotate_tor_circuit",
    "_get_exit_ip",
    "_mark_exit_bad",
    "_mark_exit_good",
    "_record_captcha_hit",
    "_cleanup_expired_blocks",
    "_cleanup_expired_proxy_blocks_db",
    "_get_blocklist_status",
]
