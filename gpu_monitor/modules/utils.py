#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import print_function

import json
import logging
import os
import subprocess
import tempfile
import time


LOG = logging.getLogger(
    "gpu_monitor.utils"
)


class CommandError(Exception):
    pass


def run_command(
        command,
        timeout=60,
        stdin_data=None):

    LOG.debug(
        "Executing command: %s",
        " ".join(command)
    )

    try:

        process = subprocess.Popen(
            command,
            stdin=(
                subprocess.PIPE
                if stdin_data is not None
                else None
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:

            stdout, stderr = process.communicate(
                stdin_data.encode("utf-8")
                if stdin_data is not None
                else None
            )

        except Exception:

            process.kill()
            process.wait()
            raise

    except OSError as exc:

        raise CommandError(
            "Unable to execute '{}': {}".format(
                command[0],
                exc
            )
        )

    stdout = stdout.decode(
        "utf-8",
        "replace"
    )

    stderr = stderr.decode(
        "utf-8",
        "replace"
    )

    if process.returncode != 0:

        raise CommandError(
            "Command failed rc={} command='{}' stderr='{}'".format(
                process.returncode,
                " ".join(command),
                stderr.strip()
            )
        )

    return stdout


def parse_runtime(value):

    if not value:
        return 0

    value = str(value)

    value = value.split(
        ".",
        1
    )[0]

    days = 0

    if "-" in value:

        day_string, value = value.split(
            "-",
            1
        )

        try:
            days = int(day_string)

        except ValueError:
            return 0

    parts = value.split(":")

    try:

        if len(parts) == 2:

            return (
                days * 86400
                + int(parts[0]) * 60
                + int(parts[1])
            )

        if len(parts) == 3:

            return (
                days * 86400
                + int(parts[0]) * 3600
                + int(parts[1]) * 60
                + int(parts[2])
            )

    except ValueError:

        return 0

    return 0


def time_to_hours(value):

    return (
        float(
            parse_runtime(value)
        )
        / 3600.0
    )


def size_to_bytes(value):

    if value is None:
        return 0.0

    value = str(value).strip()

    if not value:
        return 0.0

    units = {
        "K": 1024.0,
        "M": 1024.0 ** 2,
        "G": 1024.0 ** 3,
        "T": 1024.0 ** 4,
        "P": 1024.0 ** 5,
    }

    suffix = value[-1].upper()

    if suffix in units:

        try:
            number = float(
                value[:-1]
            )

        except ValueError:
            return 0.0

        return number * units[suffix]

    try:

        return float(value)

    except ValueError:

        return 0.0


def size_to_gb(value):

    return (
        size_to_bytes(value)
        / (1024.0 ** 3)
    )


def size_to_kb(value):

    return (
        size_to_bytes(value)
        / 1024.0
    )


def human_size(
        value,
        decimal_places=1):

    if isinstance(
            value,
            str):

        number = size_to_bytes(
            value
        )

    else:

        number = float(
            value
        )

    units = [
        "B",
        "K",
        "M",
        "G",
        "T",
        "P",
    ]

    index = 0

    while (
        number >= 1024.0
        and index < len(units) - 1
    ):

        number /= 1024.0
        index += 1

    return "{:.{}f}{}".format(
        number,
        decimal_places,
        units[index]
    )


class FileLock(object):

    def __init__(
            self,
            filename):

        self.filename = filename
        self.fd = None

    def acquire(self):

        directory = os.path.dirname(
            self.filename
        )

        if (
            directory
            and not os.path.isdir(directory)
        ):

            os.makedirs(
                directory
            )

        try:

            import fcntl

        except ImportError:

            return

        self.fd = open(
            self.filename,
            "w"
        )

        try:

            fcntl.flock(
                self.fd.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB
            )

        except IOError:

            self.fd.close()
            self.fd = None

            raise RuntimeError(
                "Another GPU monitor instance is already running"
            )

        self.fd.write(
            "{}\n".format(
                os.getpid()
            )
        )

        self.fd.flush()

    def release(self):

        if self.fd is None:
            return

        try:

            import fcntl

            fcntl.flock(
                self.fd.fileno(),
                fcntl.LOCK_UN
            )

        except Exception:
            pass

        try:
            self.fd.close()

        except Exception:
            pass

        self.fd = None


class StateStore(object):

    def __init__(
            self,
            filename):

        self.filename = filename

    def load(self):

        if not os.path.exists(
                self.filename):

            return {}

        try:

            with open(
                    self.filename,
                    "r"
            ) as handle:

                data = json.load(
                    handle
                )

            if isinstance(
                    data,
                    dict):

                return data

        except Exception as exc:

            LOG.warning(
                "Unable to load state file %s: %s",
                self.filename,
                exc
            )

        return {}

    def save(
            self,
            state):

        directory = os.path.dirname(
            self.filename
        )

        if (
            directory
            and not os.path.isdir(directory)
        ):

            os.makedirs(
                directory
            )

        temporary = (
            self.filename
            +
            ".tmp"
        )

        with open(
                temporary,
                "w"
        ) as handle:

            json.dump(
                state,
                handle,
                indent=2,
                sort_keys=True
            )

        os.rename(
            temporary,
            self.filename
        )


def get_user_email(
        username,
        getent_command="getent",
        timeout=10):

    if not username:
        return None

    command = [
        getent_command,
        "passwd",
        username
    ]

    try:

        output = run_command(
            command,
            timeout=timeout
        )

    except Exception as exc:

        LOG.warning(
            "Unable to query user %s: %s",
            username,
            exc
        )

        return None

    # --------------------------------------------------------------
    # First try passwd/GECOS email-like information.
    # --------------------------------------------------------------

    line = output.strip()

    if not line:
        return None

    fields = line.split(":")

    if len(fields) >= 5:

        gecos = fields[4]

        for item in gecos.split(","):

            item = item.strip()

            if (
                "@" in item
                and " " not in item
            ):

                return item

    # --------------------------------------------------------------
    # Site-specific fallback:
    # username@sxx.yy
    # --------------------------------------------------------------

    return "{}@xx.yy".format(
        username
    )

