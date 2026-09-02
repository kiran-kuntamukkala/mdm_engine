from __future__ import annotations

import re
from typing import Any, Dict

from functions.utils import get_logger

logger = get_logger(__name__)


def standardize_name(value: Any) -> str | None:
    """Normalize person or entity names into a canonical uppercase form."""
    try:
        if value is None:
            return None
        cleaned = re.sub(r"\s+", " ", str(value).strip())
        if not cleaned:
            return None
        return cleaned.upper()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Name standardization failed for value '%s': %s", value, exc)
        return None


def standardize_email(value: Any) -> str | None:
    """Normalize email addresses to a lower-case canonical format."""
    try:
        if value is None:
            return None
        cleaned = str(value).strip().lower()
        if not cleaned:
            return None
        return cleaned
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Email standardization failed for value '%s': %s", value, exc)
        return None


def standardize_phone(value: Any) -> str | None:
    """Normalize phone numbers to digits only, removing formatting and common country code noise."""
    try:
        if value is None:
            return None

        digits = re.sub(r"\D+", "", str(value).strip())
        if not digits:
            return None

        if len(digits) > 10 and digits.startswith("91"):
            digits = digits[2:]
        elif len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]

        if digits.startswith("0") and len(digits) > 10:
            digits = digits.lstrip("0")

        return digits
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Phone standardization failed for value '%s': %s", value, exc)
        return None


def standardize_address(value: Any) -> str | None:
    """Normalize free-form address strings into a single-space canonical form."""
    try:
        if value is None:
            return None
        cleaned = re.sub(r"\s+", " ", str(value).strip())
        if not cleaned:
            return None
        return cleaned.title()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("Address standardization failed for value '%s': %s", value, exc)
        return None


def standardize_by_classification(value: Any, classification: str) -> str | None:
    """Apply a standardization transformation based on the resolved column classification."""
    try:
        if value is None:
            return None

        mapping: Dict[str, Any] = {
            "NAME": standardize_name,
            "EMAIL": standardize_email,
            "PHONE": standardize_phone,
            "ADDRESS": standardize_address,
        }

        standardizer = mapping.get(classification)
        if standardizer is None:
            return str(value).strip()

        return standardizer(value)
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception(
            "Classification-driven standardization failed for classification '%s' and value '%s': %s",
            classification,
            value,
            exc,
        )
        return None
