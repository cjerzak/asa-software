"""Optional schema-specific profiles for domain heuristics.

Core agent behavior should remain task-agnostic. Domain profiles are loaded
only when explicitly enabled by caller configuration.
"""

from typing import Any, Dict


_ELITE_BIO_PROFILE: Dict[str, Any] = {
    "semantic_token_aliases": {
        "indigenous": {"indigenous", "indigena", "originario"},
        "community": {"community", "comunidad", "comunitaria", "comunitario"},
        "member": {"member", "miembro", "habitante", "representante"},
        "leader": {"leader", "dirigente", "lider", "lidera"},
    },
    "class_background_hints": {
        "Working class": (
            "agricult",
            "artisan",
            "campesin",
            "clerical",
            "clerk",
            "community member",
            "driver",
            "farmer",
            "fisher",
            "indigenous",
            "labor",
            "manual",
            "peasant",
            "service worker",
            "trade",
            "vendor",
        ),
        "Middle class/professional": (
            "academic",
            "accountant",
            "attorney",
            "civil servant",
            "doctor",
            "economist",
            "engineer",
            "journalist",
            "lawyer",
            "manager",
            "nurse",
            "professor",
            "teacher",
        ),
        "Upper/elite": (
            "aristocrat",
            "ceo",
            "chairman",
            "executive",
            "industrialist",
            "landowner",
            "magnate",
            "owner",
            "tycoon",
        ),
    },
    "birth_place_context_markers": (
        "nacio",
        "nacida",
        "lugar de nacimiento",
        "birth place",
        "born in",
    ),
}


_PROFILES: Dict[str, Dict[str, Any]] = {
    "elite_bio": _ELITE_BIO_PROFILE,
}


def get_schema_profile(name: str) -> Dict[str, Any]:
    """Return a schema profile map or an empty map when not found."""
    profile_name = str(name or "").strip().lower()
    profile = _PROFILES.get(profile_name)
    if not isinstance(profile, dict):
        return {}
    return profile
