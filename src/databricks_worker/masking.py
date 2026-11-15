"""Keep credentials out of query results.

Job settings and cluster specs can carry configuration maps (``spark_conf``,
``spark_env_vars``, notebook ``base_parameters``, job ``parameters``). Databricks
recommends ``{{secrets/<scope>/<key>}}`` references there, but nothing stops a
plaintext password. Any value whose key looks secret, or whose value looks
like a credential, is replaced by ``'***'`` before it reaches DuckDB. Secret
*references* are kept: they name a scope and key, not the value.
"""

from __future__ import annotations

import re
from typing import Any

MASK = "***"

_SECRET_KEY = re.compile(
    r"(pass(word|wd)?|pwd|secret|token|api[_.-]?key|access[_.-]?key|private[_.-]?key|credential|auth|"
    r"sas|signature|account[_.-]?key|client[_.-]?secret|conn(ection)?[_.-]?str)",
    re.IGNORECASE,
)
_SECRET_VALUE = re.compile(
    r"^(dapi[0-9a-f]{20,}|dose[0-9a-f]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|xox[abpr]-|AKIA[0-9A-Z]{16}|"
    r"eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.|-----BEGIN [A-Z ]*PRIVATE KEY)"
)
_URL_PASSWORD = re.compile(r"://[^/@:\s]+:[^/@\s]+@")
_SECRET_REF = re.compile(r"^\{\{\s*secrets/[^}]+\}\}$")


def is_secret_key(key: str) -> bool:
    """Return True when a configuration key name suggests a credential."""
    return bool(_SECRET_KEY.search(key))


def looks_secret(value: str) -> bool:
    """Return True when a value looks like a credential (token prefixes, JWTs, keys, URLs with passwords)."""
    return bool(_SECRET_VALUE.search(value.strip()) or _URL_PASSWORD.search(value))


def mask_value(key: str, value: Any) -> Any:
    """Mask one configuration value given its key; secret references and non-strings pass through."""
    if not isinstance(value, str) or value == "":
        return value
    if _SECRET_REF.match(value.strip()):
        return value
    if is_secret_key(key) or looks_secret(value):
        return MASK
    return value


def mask_tree(value: Any, key: str = "") -> Any:
    """Recursively mask string values under secret-looking keys anywhere in a JSON document.

    ``{"name": "db_password", "default": "x"}``-style parameter objects are
    masked too: their ``default`` / ``value`` is masked when ``name`` looks secret.
    """
    if isinstance(value, dict):
        named = value.get("name") if isinstance(value.get("name"), str) else None
        out: dict[str, Any] = {}
        for k, v in value.items():
            if named is not None and k in ("default", "value") and is_secret_key(named):
                out[k] = mask_value(named, v) if isinstance(v, str) else v
            else:
                out[k] = mask_tree(v, str(k))
        return out
    if isinstance(value, list):
        return [mask_tree(v, key) for v in value]
    return mask_value(key, value)
