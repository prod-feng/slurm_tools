import re
from typing import Union


Number = Union[int, float]


def time_to_seconds(value: str) -> int:
    """
    Convert Slurm elapsed/CPU time to seconds.

    Supported formats include:

        SS
        MM:SS
        HH:MM:SS
        D-HH:MM:SS
        D-HH:MM

    Fractional seconds are ignored.
    """

    if not value:
        return 0

    value = value.strip().split(".", 1)[0]

    if "-" in value:
        days_text, time_text = value.split("-", 1)
        days = int(days_text)
    else:
        days = 0
        time_text = value

    parts = [int(x) for x in time_text.split(":")]

    if len(parts) == 1:
        hours = 0
        minutes = 0
        seconds = parts[0]

    elif len(parts) == 2:
        hours = 0
        minutes, seconds = parts

    elif len(parts) == 3:
        hours, minutes, seconds = parts

    else:
        raise ValueError(f"Invalid time value: {value}")

    return (
        days * 86400
        + hours * 3600
        + minutes * 60
        + seconds
    )


def time_to_hours(value: str) -> float:
    """Convert a Slurm time value to hours."""

    return time_to_seconds(value) / 3600.0


def size_to_bytes(value: str) -> float:
    """
    Convert a Slurm size value to bytes.

    Slurm memory/disk values commonly use:
        K, M, G, T, P

    Binary multipliers are used because Slurm's
    memory values are traditionally expressed using
    powers of 1024.
    """

    if value is None:
        return 0.0

    value = str(value).strip()

    if not value:
        return 0.0

    # Remove trailing "N/A" or similar values.
    if value.upper() in {"N/A", "NA", "NONE", "-"}:
        return 0.0

    match = re.fullmatch(
        r"([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)",
        value,
        re.IGNORECASE,
    )

    if not match:
        raise ValueError(f"Invalid size value: {value}")

    number = float(match.group(1))
    unit = match.group(2).upper()

    multipliers = {
        "": 1,
        "K": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
        "P": 1024**5,
    }

    return number * multipliers[unit]


def size_to_gb(value: str) -> float:
    """Convert a size value to GB."""

    return size_to_bytes(value) / (1024**3)


def size_to_kb(value: str) -> float:
    """Convert a size value to KB."""

    return size_to_bytes(value) / 1024


def human_size(
    value: Union[str, Number],
    decimal_places: int = 1,
) -> str:
    """
    Return a human-readable size.

    Examples:
        1024       -> 1.0K
        1048576    -> 1.0M
        1073741824 -> 1.0G

    A string such as "1.5G" is also accepted.
    """

    if isinstance(value, str):
        value = value.strip()

        match = re.fullmatch(
            r"([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)",
            value,
            re.IGNORECASE,
        )

        if not match:
            raise ValueError(f"Invalid size value: {value}")

        number = float(match.group(1))
        unit = match.group(2).upper()

        unit_multipliers = {
            "": 1,
            "K": 1024,
            "M": 1024**2,
            "G": 1024**3,
            "T": 1024**4,
            "P": 1024**5,
        }

        bytes_value = number * unit_multipliers[unit]

    else:
        bytes_value = float(value)

    units = ["B", "K", "M", "G", "T", "P"]

    size = bytes_value
    unit_index = 0

    while size >= 1024.0 and unit_index < len(units) - 1:
        size /= 1024.0
        unit_index += 1

    unit = units[unit_index]

    return f"{size:.{decimal_places}f}{unit}"


def safe_float(value: str, default: float = 0.0) -> float:
    """Safely convert a value to float."""

    if value is None or not str(value).strip():
        return default

    try:
        return float(value)
    except (TypeError, ValueError):
        return default

