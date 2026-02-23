"""Optional schema-specific profiles for domain heuristics.

Core agent behavior should remain task-agnostic. Domain profiles are loaded
only when explicitly enabled by caller configuration.
"""

from typing import Any, Dict


_PROFILES: Dict[str, Dict[str, Any]] = {}


def get_schema_profile(name: str) -> Dict[str, Any]:
    """Return a schema profile map or an empty map when not found."""
    profile_name = str(name or "").strip().lower()
    profile = _PROFILES.get(profile_name)
    if not isinstance(profile, dict):
        return {}
    return profile
