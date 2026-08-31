import os

from .emailer import EmailError, Emailer
from .report import build_job_report, parse_job_records
from .slurm import SlurmClient
from .utils import (
    default_end_time,
    default_start_time,
    get_config_list,
)


def determine_recipient(slurm, emailer, username):
    """
    Determine the best email address for a user.

    Priority:

    1. Slurm MailUser
       (not available at this stage because this function receives
       a username rather than a job)
    2. getent passwd
    3. configured fallback recipient
    """
    address = slurm.passwd_email(username)

    if address:
        return address

    return emailer.fallback_recipient


def run_job_report(args, config):
    """
    Main job-report workflow.
    """
    slurm = SlurmClient(config)

    emailer = None

    if args.emailusers:
        emailer = Emailer(config)

    # --------------------------------------------------------------
    # Time range
    # --------------------------------------------------------------
    if args.starttime:
        start = args.starttime
    else:
        try:
            days = config.getint(
                "jobs",
                "default_days",
                fallback=1,
            )
        except ValueError:
            days = 1

        start = default_start_time(days)

    end = (
        args.endtime
        if args.endtime
        else default_end_time()
    )

    # --------------------------------------------------------------
    # User selection
    # --------------------------------------------------------------
    user = args.user

    allusers = args.allusers

    if not user and not allusers and not args.jobs:
        user = os.environ.get(
            "USER",
            os.environ.get("USERNAME", ""),
        )

    # --------------------------------------------------------------
    # State
    # --------------------------------------------------------------
    state = None

    if not args.allstates:
        if args.state:
            state = args.state
        else:
            state = config.get(
                "jobs",
                "default_state",
                fallback="CD",
            ).strip()

    # --------------------------------------------------------------
    # Query Slurm
    # --------------------------------------------------------------
    result = slurm.sacct_jobs(
        user=user,
        all_users=allusers,
        jobs=args.jobs,
        start=start,
        end=end,
        state=state,
    )

    records = parse_job_records(result)

    if not records:
        print("No records found. Please try again.")
        return 0

    ignored_partitions = get_config_list(
        config,
        "jobs",
        "ignored_partitions",
    )

    if ignored_partitions:
        for username in list(records.keys()):
            records[username] = {
                job_id: job
                for job_id, job in records[username].items()
                if job["partition"] not in ignored_partitions
            }

            if not records[username]:
                del records[username]

    if not records:
        print("No jobs remain after filtering.")
        return 0

    try:
        default_njobs = config.getint(
            "jobs",
            "default_njobs",
            fallback=10,
        )
    except ValueError:
        default_njobs = 10

    max_jobs = args.njobs

    if max_jobs is None or max_jobs < 1:
        max_jobs = default_njobs

    reports = build_job_report(
        records,
        max_jobs=max_jobs,
        csv=args.csv,
    )

    print(
        "Found {} user(s) with matching jobs.".format(
            len(records)
        )
    )

    # --------------------------------------------------------------
    # CSV output
    # --------------------------------------------------------------
    if args.csv:
        first = True

        for username in sorted(reports):
            report = reports[username]

            lines = report.splitlines()

            if first:
                print("\n".join(lines))
                first = False
            else:
                # Avoid repeating the header for every user.
                print("\n".join(lines[1:]))

        return 0

    # --------------------------------------------------------------
    # Normal console output / email
    # --------------------------------------------------------------
    for username in sorted(reports):
        report = reports[username]

        print()
        print(report)

        if not args.emailusers:
            continue

        recipient = determine_recipient(
            slurm,
            emailer,
            username,
        )

        if not recipient:
            print(
                "WARNING: No email address found for {}.".format(
                    username
                )
            )
            continue

        variables = {
            "user": username,
            "job_report": report,
            "job_count": len(records[username]),
            "start_time": start,
            "end_time": end,
        }

        try:
            print(
                "Sending job report to {} ({})...".format(
                    username,
                    recipient,
                )
            )

            emailer.send_job_report(
                recipient,
                variables,
            )

        except EmailError as exc:
            print(
                "ERROR sending email to {}: {}".format(
                    username,
                    exc,
                )
            )

    return 0

