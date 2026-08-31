import configparser
import os
import re
from datetime import datetime, timedelta


def project_root():
    """
    Return the root directory of the slurm_report project.
    """
    return os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )


def load_config(config_path=None):
    """
    Load the application configuration.

    If config_path is not specified, use slurm_report.conf
    from the project root.
    """
    if config_path is None:
        config_path = os.path.join(
            project_root(),
            "slurm_report.conf",
        )

    if not os.path.isfile(config_path):
        raise RuntimeError(
            "Configuration file not found: {}".format(config_path)
        )

    config = configparser.ConfigParser()
    config.read(config_path)

    return config


def get_config_path(config, section, option, fallback=""):
    """
    Return a configuration value and expand environment variables.
    """
    value = config.get(
        section,
        option,
        fallback=fallback,
    )

    return os.path.expandvars(os.path.expanduser(value.strip()))


def get_config_list(config, section, option, fallback=""):
    """
    Return a comma-separated configuration value as a list.
    """
    value = get_config_path(
        config,
        section,
        option,
        fallback,
    )

    if not value:
        return []

    return [
        item.strip()
        for item in value.split(",")
        if item.strip()
    ]


def default_start_time(days=1):
    """
    Return an ISO timestamp representing N days ago.
    """
    return (
        datetime.now() - timedelta(days=days)
    ).isoformat(
        sep="T",
        timespec="seconds",
    )


def default_end_time():
    """
    Return the current local time in ISO format.
    """
    return datetime.now().isoformat(
        sep="T",
        timespec="seconds",
    )


def parse_slurm_time(value):
    """
    Convert common Slurm elapsed-time formats to seconds.

    Supported examples:

        00:00:08
        01:02:03
        1-05:11:54
        05:11
        3600

    Empty or invalid values return 0.
    """
    if value is None:
        return 0.0

    value = str(value).strip()

    if not value:
        return 0.0

    value = value.split(".")[0]

    try:
        if "-" in value:
            days, time_part = value.split("-", 1)
            days = int(days)
        else:
            days = 0
            time_part = value

        parts = time_part.split(":")

        if len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = int(parts[2])

        elif len(parts) == 2:
            hours = 0
            minutes = int(parts[0])
            seconds = int(parts[1])

        elif len(parts) == 1:
            hours = 0
            minutes = 0
            seconds = int(parts[0])

        else:
            return 0.0

        return (
            days * 86400
            + hours * 3600
            + minutes * 60
            + seconds
        )

    except (ValueError, TypeError):
        return 0.0


def time2hours(value):
    """
    Convert a Slurm elapsed time to hours.
    """
    return parse_slurm_time(value) / 3600.0


def parse_size_to_bytes(value):
    """
    Convert a Slurm size string to bytes.

    Examples:

        100K
        1.5M
        10G
        2T
        1000000

    Slurm memory/storage values are commonly binary-oriented,
    so powers of 1024 are used here.
    """
    if value is None:
        return 0.0

    value = str(value).strip()

    if not value:
        return 0.0

    value = value.replace(" ", "")

    match = re.match(
        r"^([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?)i?B?$",
        value,
        re.IGNORECASE,
    )

    if not match:
        try:
            return float(value)
        except ValueError:
            return 0.0

    number = float(match.group(1))
    unit = match.group(2).upper()

    multipliers = {
        "": 1,
        "K": 1024,
        "M": 1024 ** 2,
        "G": 1024 ** 3,
        "T": 1024 ** 4,
        "P": 1024 ** 5,
        "E": 1024 ** 6,
    }

    return number * multipliers[unit]


def size2GB(value):
    """
    Convert a size to GB.

    Uses binary units internally.
    """
    return parse_size_to_bytes(value) / float(1024 ** 3)


def size2KB(value):
    """
    Convert a size to KB.
    """
    return parse_size_to_bytes(value) / float(1024)


def human_bytes(value, decimal_places=1):
    """
    Convert bytes to a human-readable value.
    """
    try:
        value = float(value)
    except (ValueError, TypeError):
        return "0B"

    units = ["B", "K", "M", "G", "T", "P"]

    for unit in units:
        if abs(value) < 1024.0 or unit == units[-1]:
            return "{:.{}f}{}".format(
                value,
                decimal_places,
                unit,
            )

        value /= 1024.0

    return "{:.{}f}P".format(
        value,
        decimal_places,
    )


def human_size(value, decimal_places=1):
    """
    Convert a Slurm size string into human-readable format.

    Examples:

        1000K -> 1000.0K
        2G    -> 2.0G
        1024M -> 1.0G
    """
    return human_bytes(
        parse_size_to_bytes(value),
        decimal_places,
    )


def safe_float(value, default=0.0):
    """
    Convert a value to float safely.
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    """
    Convert a value to int safely.
    """
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def contains_any(value, patterns):
    """
    Return True if any pattern occurs in value.
    """
    value = str(value).lower()

    for pattern in patterns:
        if pattern.lower() in value:
            return True

    return False


def escape_csv(value):
    """
    Escape a value for simple CSV output.
    """
    value = "" if value is None else str(value)

    if any(char in value for char in [",", '"', "\n"]):
        value = value.replace('"', '""')
        return '"{}"'.format(value)

    return value

