#!/usr/bin/env python3

import argparse
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

from src.slurm import get_jobs
from src.report import build_reports
from src.emailer import Emailer


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_TEMPLATE = PROJECT_DIR / "job_report.html"
DEFAULT_CONFIG = PROJECT_DIR / "job_report.conf"


def parse_arguments():
    """Parse command-line arguments."""

    uid = os.environ.get("USER", os.environ.get("USERNAME", ""))

    start = (
        datetime.today() - timedelta(days=1)
    ).isoformat(sep="T", timespec="seconds")

    end = datetime.today().isoformat(
        sep="T",
        timespec="seconds",
    )

    parser = argparse.ArgumentParser(
        description=(
            "Calculate CPU, memory and disk usage for "
            "Slurm jobs."
        )
    )

    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "-u",
        "--user",
        default=uid,
        help=(
            "Comma-separated list of UIDs or usernames "
            "to select jobs."
        ),
    )

    group.add_argument(
        "--allusers",
        action="store_true",
        help="Display all users' jobs.",
    )

    group.add_argument(
        "-j",
        "--jobs",
        help=(
            "Specified job[.step] or comma-separated "
            "list of jobs."
        ),
    )

    parser.add_argument(
        "-S",
        "--starttime",
        default=start,
        help=(
            "Select jobs after this time. "
            "YYYY-MM-DD[THH:MM[:SS]]"
        ),
    )

    parser.add_argument(
        "-E",
        "--endtime",
        default=end,
        help=(
            "Select jobs before this time. "
            "YYYY-MM-DD[THH:MM[:SS]]"
        ),
    )

    parser.add_argument(
        "-n",
        "--njobs",
        type=int,
        default=10,
        help=(
            "Number of randomly selected jobs per user "
            "when generating the normal report."
        ),
    )

    parser.add_argument(
        "-s",
        "--state",
        default="CD",
        help="Select jobs in a specific state. Default: CD",
    )

    parser.add_argument(
        "--allstates",
        action="store_true",
        help="Display jobs in all states.",
    )

    parser.add_argument(
        "--csv",
        action="store_true",
        help="Write the report in CSV format.",
    )

    parser.add_argument(
        "--emailusers",
        action="store_true",
        help="Send the report to users by email.",
    )

    parser.add_argument(
        "--template",
        type=Path,
        default=DEFAULT_TEMPLATE,
        help=(
            f"HTML email template. "
            f"Default: {DEFAULT_TEMPLATE}"
        ),
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=(
            f"Email configuration file. "
            f"Default: {DEFAULT_CONFIG}"
        ),
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.njobs < 1:
        args.njobs = 10

    try:
        jobs = get_jobs(
            user=args.user,
            all_users=args.allusers,
            jobs=args.jobs,
            start_time=args.starttime,
            end_time=args.endtime,
            state=args.state,
            all_states=args.allstates,
        )

    except Exception as exc:
        print(f"ERROR: Unable to query Slurm: {exc}", file=sys.stderr)
        return 1

    if not jobs:
        print("No records found. Please try again.")
        return 0

    print(f"Found {len(jobs)} job records")

    reports = build_reports(
        jobs=jobs,
        max_jobs=args.njobs,
        csv=args.csv,
        random_module=random,
    )

    if args.csv:
        for report in reports.values():
            print(report)
        return 0

    emailer = None

    if args.emailusers:
        try:
            emailer = Emailer(args.config)
        except Exception as exc:
            print(
                f"ERROR: Unable to load email configuration: {exc}",
                file=sys.stderr,
            )
            return 1

    for user, report in reports.items():

        if report.reportable_jobs == 0:
            continue

        if args.emailusers:
            try:
                body = emailer.render_template(
                    args.template,
                    user=user,
                    num_report=report.num_report,
                    total_jobs=report.total_jobs,
                    job_info=report.job_info,
                )

                print(
                    f"Sending email to {user}: "
                    f"{report.email_address}"
                )

                emailer.send(
                    recipient=report.email_address,
                    body=body,
                )

            except Exception as exc:
                print(
                    f"ERROR: Failed to send email to "
                    f"{user}: {exc}",
                    file=sys.stderr,
                )

        else:
            print(report.job_info)

    return 0


if __name__ == "__main__":
    sys.exit(main())

