import re
from dataclasses import dataclass


@dataclass
class RedactionResult:
    redacted_text: str
    redaction_count: int
    patterns_hit: list[str]


_PATTERNS: list[tuple[str, str, str]] = [
    (
        "ssn",
        r"\b(?!000|666|9\d{2})\d{3}[-\s]?(?!00)\d{2}[-\s]?(?!0000)\d{4}\b",
        "[SSN REDACTED]",
    ),
    (
        "credit_card",
        r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b",
        "[CARD REDACTED]",
    ),
    (
        "bank_account",
        r"\b(?:routing|acct|account)[\s#:]*\d{4,17}\b",
        "[ACCOUNT REDACTED]",
    ),
    (
        "bare_account_number",
        r"(?<!\d)\d{9,17}(?!\d)",
        "[ACCOUNT REDACTED]",
    ),
    (
        "dob",
        r"\b(?:dob|date of birth|born)[\s:]*\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}\b",
        "[DOB REDACTED]",
    ),
    (
        "medicare_id",
        r"\b[1-9][A-Za-z][A-Za-z0-9]\d[A-Za-z][A-Za-z0-9]\d[A-Za-z]{2}\d{2}\b",
        "[MEDICARE ID REDACTED]",
    ),
    (
        "passport",
        r"\b[A-Z]{1,2}\d{6,9}\b",
        "[PASSPORT REDACTED]",
    ),
    (
        "drivers_license",
        r"\b(?:dl|drv\s?lic|driver'?s?\s?lic(?:ense)?)[:\s]*[A-Z0-9\-]{6,15}\b",
        "[DL REDACTED]",
    ),
]

_COMPILED = [
    (label, re.compile(pattern, re.IGNORECASE), replacement)
    for label, pattern, replacement in _PATTERNS
]


def redact_pii(text: str) -> RedactionResult:
    result = text
    count = 0
    patterns_hit = []

    for label, pattern, replacement in _COMPILED:
        new_result, n = pattern.subn(replacement, result)
        if n > 0:
            count += n
            patterns_hit.append(f"{label}({n})")
            result = new_result

    return RedactionResult(
        redacted_text=result,
        redaction_count=count,
        patterns_hit=patterns_hit,
    )


def redact_for_api(text: str, max_chars: int = 6000) -> tuple[str, dict]:
    result = redact_pii(text)
    truncated = result.redacted_text[:max_chars]
    was_truncated = len(result.redacted_text) > max_chars

    return truncated, {
        "redaction_count": result.redaction_count,
        "patterns_hit": result.patterns_hit,
        "chars_sent": len(truncated),
        "was_truncated": was_truncated,
    }
