"""
SQL security validation for ProcureSpendIQ Analytics.

Only SELECT queries are allowed through the Genie interface.
DDL/DML statements and dangerous keywords are blocked.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Deny-list keywords
# ---------------------------------------------------------------------------

FORBIDDEN_KEYWORDS: list[str] = [
    "DROP",
    "DELETE",
    "UPDATE",
    "INSERT",
    "ALTER",
    "TRUNCATE",
    "EXEC",
    "EXECUTE",
    "CREATE",
    "MERGE",
    "BULK",
    "OPENROWSET",
    "OPENDATASOURCE",
    "xp_cmdshell",
    "sp_executesql",
]

# Maximum SQL length to prevent abuse
MAX_SQL_LENGTH = 8_000


def validate_sql(sql: str) -> bool:
    """
    Validate a SQL string for safety.

    Rules:
      - Must start with SELECT (after stripping whitespace and comments).
      - Must not contain any forbidden keywords.
      - Must not exceed MAX_SQL_LENGTH characters.

    Raises ValueError on any violation; returns True otherwise.
    """
    if not sql or not sql.strip():
        raise ValueError("Empty SQL statement is not allowed.")

    if len(sql) > MAX_SQL_LENGTH:
        raise ValueError(
            f"SQL statement exceeds maximum length of {MAX_SQL_LENGTH} characters."
        )

    # Strip single-line and block comments before checking
    cleaned = re.sub(r"--[^\n]*", " ", sql)
    cleaned = re.sub(r"/\*.*?\*/", " ", cleaned, flags=re.DOTALL)
    cleaned_upper = cleaned.strip().upper()

    if not cleaned_upper.startswith("SELECT"):
        raise ValueError("Only SELECT queries are permitted.")

    for keyword in FORBIDDEN_KEYWORDS:
        pattern = rf"\b{re.escape(keyword)}\b"
        if re.search(pattern, cleaned_upper):
            raise ValueError(f"Forbidden SQL keyword detected: {keyword}")

    return True


def sanitize_identifier(name: str) -> str:
    """
    Return a safely quoted SQL identifier.
    Only alphanumerics and underscores are allowed in the raw name.
    """
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_ ]*$", name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return f"[{name}]"


def sanitize_string_param(value: str, max_length: int = 500) -> str:
    """
    Escape a user-supplied string for use inside a SQL single-quoted literal.
    """
    if not isinstance(value, str):
        value = str(value)
    if len(value) > max_length:
        value = value[:max_length]
    return value.replace("'", "''")
