#!/usr/bin/env python3
#
# Analyze Slurm account hierarchy, users, PIs and project accounts.
# ============================
#          ACCOUNT TREE
# ============================
##root
##└── dept_technology_and_society
##    └── pi_feng
##        ├── pn_cs
##        │   ├── @feng
##        │   └── @zhang
##        └── pn_math
##            └── @li
#
import argparse
import fnmatch
import subprocess
import sys
from collections import defaultdict


SACCTMGR_COMMAND = [
    "sacctmgr",
    "-n",
    "-P",
    "show",
    "assoc",
    "format=Account,ParentName,User",
]


# ----------------------------------------------------------------------
# Get data from Slurm
# ----------------------------------------------------------------------

def get_sacctmgr_data():
    """Get account/user associations directly from Slurm."""

    try:
        result = subprocess.run(
            SACCTMGR_COMMAND,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

    except FileNotFoundError:
        print(
            "ERROR: sacctmgr command was not found.",
            file=sys.stderr,
        )
        sys.exit(1)

    except subprocess.CalledProcessError as exc:
        print(
            "ERROR: sacctmgr failed:",
            file=sys.stderr,
        )

        if exc.stderr:
            print(exc.stderr, file=sys.stderr)

        sys.exit(1)

    return result.stdout


# ----------------------------------------------------------------------
# Parse sacctmgr output
# ----------------------------------------------------------------------

def parse_sacctmgr_output(output):
    """
    Parse:

        Account|ParentName|User|

    Examples:

        dept_foo|root||
        pi_feng|dept_foo||
        pn_cs|pi_feng||
        pn_cs||feng|
        pn_cs||zhang|
    """

    records = []

    for line_num, line in enumerate(output.splitlines(), 1):

        line = line.strip()

        if not line:
            continue

        fields = line.split("|")

        fields += [""] * (3 - len(fields))

        account = fields[0].strip()
        parent = fields[1].strip()
        user = fields[2].strip()

        if not account:
            continue

        records.append(
            {
                "account": account,
                "parent": parent,
                "user": user,
                "line": line_num,
            }
        )

    return records


# ----------------------------------------------------------------------
# Account information
# ----------------------------------------------------------------------

def get_accounts(records):
    """
    Return:

        account -> parent

    Only rows containing ParentName define hierarchy.
    """

    accounts = {}

    for record in records:
        account = record["account"]
        parent = record["parent"]

        if not parent:
            continue

        if account in accounts:
            if accounts[account] != parent:
                print(
                    "WARNING: account has multiple parents: "
                    f"{account}: "
                    f"{accounts[account]} / {parent}",
                    file=sys.stderr,
                )
        else:
            accounts[account] = parent

    return accounts


def get_users(records):
    """
    Return:

        account -> set(users)
    """

    users = defaultdict(set)

    for record in records:
        account = record["account"]
        user = record["user"]

        if user:
            users[account].add(user)

    return users


def classify_account(account):
    """Classify an account by its prefix."""

    if account.startswith("dept_"):
        return "dept"

    if account.startswith("pi_"):
        return "pi"

    if account.startswith("pn_"):
        return "pn"

    return "other"


# ----------------------------------------------------------------------
# Wildcard matching
# ----------------------------------------------------------------------

def matches(value, pattern):
    """
    Match using shell-style wildcards.

    Examples:

        pi_*       -> all pi accounts
        pi_f*      -> pi accounts beginning with pi_f
        *feng*     -> anything containing feng
        pn_cs      -> exact match
    """

    return fnmatch.fnmatchcase(value, pattern)


def filter_accounts(accounts, pattern):
    """Return accounts matching a wildcard pattern."""

    return {
        account: parent
        for account, parent in accounts.items()
        if matches(account, pattern)
    }


def filter_users(users, pattern):
    """
    Return:

        account -> matching users
    """

    filtered = defaultdict(set)

    for account, account_users in users.items():

        for user in account_users:

            if matches(user, pattern):
                filtered[account].add(user)

    return filtered


# ----------------------------------------------------------------------
# Determine selected accounts
# ----------------------------------------------------------------------

def get_selected_accounts(
    records,
    dept_pattern=None,
    pi_pattern=None,
    pn_pattern=None,
):
    """
    Select accounts based on dept/pi/pn wildcard filters.

    If multiple filters are supplied, they are combined with OR
    semantics.

    For example:

        --pi-filter 'pi_f*' --pn 'pn_cs'

    selects matching PIs and matching projects.
    """

    accounts = get_accounts(records)

    if not any(
        [
            dept_pattern,
            pi_pattern,
            pn_pattern,
        ]
    ):
        return accounts

    selected = {}

    for account, parent in accounts.items():

        account_type = classify_account(account)

        if (
            account_type == "dept"
            and dept_pattern
            and matches(account, dept_pattern)
        ):
            selected[account] = parent

        elif (
            account_type == "pi"
            and pi_pattern
            and matches(account, pi_pattern)
        ):
            selected[account] = parent

        elif (
            account_type == "pn"
            and pn_pattern
            and matches(account, pn_pattern)
        ):
            selected[account] = parent

    return selected


# ----------------------------------------------------------------------
# Overall summary
# ----------------------------------------------------------------------

def print_summary(records):

    accounts = get_accounts(records)
    users = get_users(records)

    counts = defaultdict(int)

    for account in accounts:
        counts[classify_account(account)] += 1

    unique_users = set()

    for account_users in users.values():
        unique_users.update(account_users)

    total_associations = sum(
        len(account_users)
        for account_users in users.values()
    )

    print("=" * 70)
    print("SLURM ACCOUNT SUMMARY")
    print("=" * 70)

    print(f"Total accounts        : {len(accounts)}")
    print(f"  dept_*              : {counts['dept']}")
    print(f"  pi_*                : {counts['pi']}")
    print(f"  pn_*                : {counts['pn']}")
    print(f"  other               : {counts['other']}")

    print()
    print(f"Unique users          : {len(unique_users)}")
    print(f"User associations     : {total_associations}")

    print()


# ----------------------------------------------------------------------
# Department summary
# ----------------------------------------------------------------------

def get_department_information(records):
    """
    Build:

        department -> {
            pis,
            projects,
            users
        }
    """

    accounts = get_accounts(records)
    users = get_users(records)

    departments = {}

    for account, parent in accounts.items():

        if classify_account(account) != "dept":
            continue

        departments[account] = {
            "pis": set(),
            "projects": set(),
            "users": set(),
        }

    for account, parent in accounts.items():

        if classify_account(account) == "pi":

            department = parent

            if department in departments:
                departments[department]["pis"].add(
                    account
                )

        elif classify_account(account) == "pn":

            pi = parent

            if pi not in accounts:
                continue

            department = accounts[pi]

            if department in departments:
                departments[department]["projects"].add(
                    account
                )

                departments[department]["users"].update(
                    users.get(account, set())
                )

    return departments


def print_department_summary(records, pattern=None):

    departments = get_department_information(records)

    if pattern:
        departments = {
            dept: info
            for dept, info in departments.items()
            if matches(dept, pattern)
        }

    print("=" * 70)
    print("DEPARTMENT SUMMARY")
    print("=" * 70)

    if not departments:
        print("No matching departments found.")
        print()
        return

    rows = []

    for dept in sorted(departments):

        info = departments[dept]

        rows.append(
            (
                dept,
                len(info["pis"]),
                len(info["projects"]),
                len(info["users"]),
            )
        )

    dept_width = max(
        len("Department"),
        max(len(row[0]) for row in rows),
    ) + 2

    pis_width = max(
        len("PIs"),
        max(len(str(row[1])) for row in rows),
    )

    projects_width = max(
        len("Projects"),
        max(len(str(row[2])) for row in rows),
    )

    users_width = max(
        len("Users"),
        max(len(str(row[3])) for row in rows),
    )

    header = (
        f"{'Department':<{dept_width}}"
        f"{'PIs':>{pis_width}}"
        f"{'Projects':>{projects_width}}"
        f"{'Users':>{users_width}}"
    )

    print(header)
    print("-" * len(header))

    for dept, pis, projects, users in rows:

        print(
            f"{dept:<{dept_width}}"
            f"{pis:>{pis_width}}"
            f"{projects:>{projects_width}}"
            f"{users:>{users_width}}"
        )

    print()


# ----------------------------------------------------------------------
# PI-centric analysis
# ----------------------------------------------------------------------

def get_pi_information(records):

    accounts = get_accounts(records)
    users = get_users(records)

    pi_info = {}

    for account, parent in accounts.items():

        if classify_account(account) != "pi":
            continue

        pi_info[account] = {
            "department": parent,
            "projects": set(),
            "users": set(users.get(account, set())),
            "project_users": defaultdict(set),
        }

    for account, parent in accounts.items():

        if classify_account(account) != "pn":
            continue

        pi = parent

        if pi not in pi_info:
            continue

        pi_info[pi]["projects"].add(account)

        project_users = users.get(
            account,
            set(),
        )

        pi_info[pi]["project_users"][account].update(
            project_users
        )

        pi_info[pi]["users"].update(
            project_users
        )

    return pi_info


def print_pi_summary(records, pattern=None):

    pi_info = get_pi_information(records)

    if pattern:
        pi_info = {
            pi: info
            for pi, info in pi_info.items()
            if matches(pi, pattern)
        }

    print("=" * 70)
    print("PI SUMMARY")
    print("=" * 70)

    if not pi_info:
        print("No matching pi_* accounts found.")
        print()
        return

    total_projects = 0
    total_users = set()

    for pi in sorted(pi_info):

        info = pi_info[pi]

        department = info["department"]
        projects = sorted(info["projects"])
        pi_users = sorted(info["users"])

        total_projects += len(projects)
        total_users.update(pi_users)

        print()
        print(pi)
        print(f"  Department       : {department}")
        print(f"  Project accounts : {len(projects)}")
        print(f"  Unique users     : {len(pi_users)}")

        if projects:

            print("  Projects:")

            for project in projects:

                project_users = sorted(
                    info["project_users"].get(
                        project,
                        set(),
                    )
                )

                print(
                    f"    {project} "
                    f"({len(project_users)} users)"
                )

                if project_users:
                    print(
                        "      Users: "
                        + ", ".join(project_users)
                    )

        if pi_users:

            print("  All users:")

            for user in pi_users:
                print(f"    - {user}")

    print()
    print("-" * 70)
    print("PI TOTALS")
    print("-" * 70)

    print(f"Number of PIs       : {len(pi_info)}")
    print(f"Project accounts    : {total_projects}")
    print(f"Unique PI users     : {len(total_users)}")
    print()


def print_pi_compact_summary(records, pattern=None):

    pi_info = get_pi_information(records)

    if pattern:
        pi_info = {
            pi: info
            for pi, info in pi_info.items()
            if matches(pi, pattern)
        }

    print("=" * 70)
    print("PI SUMMARY (COMPACT)")
    print("=" * 70)

    if not pi_info:
        print("No matching pi_* accounts found.")
        print()
        return

    rows = []

    for pi in sorted(pi_info):

        info = pi_info[pi]

        rows.append(
            (
                pi,
                info["department"],
                len(info["projects"]),
                len(info["users"]),
            )
        )

    pi_width = max(
        len("PI"),
        max(len(row[0]) for row in rows),
    ) + 2

    department_width = max(
        len("Department"),
        max(len(row[1]) for row in rows),
    ) + 2

    projects_width = max(
        len("Projects"),
        max(len(str(row[2])) for row in rows),
    )

    users_width = max(
        len("Users"),
        max(len(str(row[3])) for row in rows),
    )

    header = (
        f"{'PI':<{pi_width}}"
        f"{'Department':<{department_width}}"
        f"{'Projects':>{projects_width}}"
        f"{'Users':>{users_width}}"
    )

    print(header)
    print("-" * len(header))

    for pi, department, projects, users in rows:

        print(
            f"{pi:<{pi_width}}"
            f"{department:<{department_width}}"
            f"{projects:>{projects_width}}"
            f"{users:>{users_width}}"
        )

    print()


# ----------------------------------------------------------------------
# Account tree
# ----------------------------------------------------------------------

def build_tree(records):

    accounts = get_accounts(records)

    children = defaultdict(set)

    for account, parent in accounts.items():
        children[parent].add(account)

    return children


def print_tree(records):

    children = build_tree(records)
    users = get_users(records)

    def recurse(
        parent,
        prefix="",
        path=None,
    ):

        if path is None:
            path = set()

        child_list = sorted(
            children.get(parent, [])
        )

        for index, account in enumerate(
            child_list
        ):

            is_last = (
                index == len(child_list) - 1
            )

            branch = (
                "└── "
                if is_last
                else "├── "
            )

            print(
                prefix
                + branch
                + account
            )

            if account in path:

                print(
                    prefix
                    + "    [CYCLE DETECTED]"
                )

                continue

            next_prefix = prefix + (
                "    "
                if is_last
                else "│   "
            )

            account_users = sorted(
                users.get(account, [])
            )

            for user_index, user in enumerate(
                account_users
            ):

                user_is_last = (
                    user_index
                    == len(account_users) - 1
                )

                user_branch = (
                    "└── "
                    if user_is_last
                    else "├── "
                )

                print(
                    next_prefix
                    + user_branch
                    + "@"
                    + user
                )

            recurse(
                account,
                next_prefix,
                path | {account},
            )

    print("root")

    recurse(
        "root",
        path={"root"},
    )


# ----------------------------------------------------------------------
# Account listing
# ----------------------------------------------------------------------

def print_accounts(records, pattern=None):

    accounts = get_accounts(records)
    users = get_users(records)

    if pattern:
        accounts = filter_accounts(
            accounts,
            pattern,
        )

    groups = defaultdict(list)

    for account in accounts:

        groups[
            classify_account(account)
        ].append(account)

    print("=" * 70)
    print("ACCOUNTS")
    print("=" * 70)

    for category in (
        "dept",
        "pi",
        "pn",
        "other",
    ):

        account_list = sorted(
            groups[category]
        )

        print(
            f"\n{category} "
            f"({len(account_list)}):"
        )

        for account in account_list:

            account_users = sorted(
                users.get(account, [])
            )

            if account_users:

                print(
                    f"  {account} "
                    f"[{len(account_users)} users]"
                )

            else:

                print(
                    f"  {account}"
                )

    print()


# ----------------------------------------------------------------------
# User listing
# ----------------------------------------------------------------------

def print_users(records, pattern=None):

    users = get_users(records)

    if pattern:
        users = filter_users(
            users,
            pattern,
        )

    print("=" * 70)
    print("USERS BY ACCOUNT")
    print("=" * 70)

    if not users:

        print("No matching users found.")
        print()

        return

    for account in sorted(users):

        account_users = sorted(
            users[account]
        )

        if not account_users:
            continue

        print(
            f"{account} "
            f"({len(account_users)} users):"
        )

        for user in account_users:

            print(
                f"  - {user}"
            )

    print()


# ----------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------

def find_orphans(records):

    accounts = get_accounts(records)

    orphans = []

    for account, parent in accounts.items():

        if (
            parent != "root"
            and parent not in accounts
        ):

            orphans.append(
                (
                    account,
                    parent,
                )
            )

    return sorted(orphans)


def validate_hierarchy(records):

    accounts = get_accounts(records)

    problems = []

    for account, parent in accounts.items():

        account_type = classify_account(
            account
        )

        if account_type == "dept":

            if parent != "root":

                problems.append(
                    f"{account}: expected "
                    f"parent root, got {parent}"
                )

        elif account_type == "pi":

            if not parent.startswith(
                "dept_"
            ):

                problems.append(
                    f"{account}: expected "
                    f"dept_* parent, "
                    f"got {parent}"
                )

        elif account_type == "pn":

            if not parent.startswith(
                "pi_"
            ):

                problems.append(
                    f"{account}: expected "
                    f"pi_* parent, "
                    f"got {parent}"
                )

    return problems


def print_validation(records):

    print("=" * 70)
    print("VALIDATION")
    print("=" * 70)

    orphans = find_orphans(records)

    if orphans:

        print("\nOrphan accounts:")

        for account, parent in orphans:

            print(
                f"  {account} -> "
                f"missing parent: {parent}"
            )

    else:

        print(
            "\nOrphan accounts: None"
        )

    problems = validate_hierarchy(
        records
    )

    if problems:

        print(
            "\nHierarchy problems:"
        )

        for problem in problems:

            print(
                f"  {problem}"
            )

    else:

        print(
            "Hierarchy problems: None"
        )

    print()


# ----------------------------------------------------------------------
# Argument parsing
# ----------------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Analyze Slurm account hierarchy, "
            "users, PIs and project accounts."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  Show everything:
    %(prog)s

  Overall summary:
    %(prog)s --summary

  All departments:
    %(prog)s --dept 'dept_*'

  Specific department:
    %(prog)s --dept 'dept_computer_science'

  Departments beginning with "dept_com":
    %(prog)s --dept 'dept_com*'

  PIs beginning with "pi_f":
    %(prog)s --pi-filter 'pi_f*'

  Specific project:
    %(prog)s --pn 'pn_cs'

  Projects beginning with "pn_cs":
    %(prog)s --pn 'pn_cs*'

  Users beginning with "feng":
    %(prog)s --user 'feng*'

  Users containing "zh":
    %(prog)s --user '*zh*'

  Compact PI report for matching PIs:
    %(prog)s --pi-compact --pi-filter 'pi_f*'

  Detailed PI report:
    %(prog)s --pi --pi-filter 'pi_f*'

  List matching accounts:
    %(prog)s --accounts --pi-filter 'pi_f*'

  List matching users:
    %(prog)s --users --user 'feng*'

  Combine filters:
    %(prog)s --pi-compact --dept 'dept_cs*'

Wildcard syntax follows Python fnmatch/shell-style matching:
    *   matches any number of characters
    ?   matches one character
    [abc] matches one character from the set
""",
    )

    # --------------------------------------------------------------
    # Reports
    # --------------------------------------------------------------

    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show overall account/user summary.",
    )

    parser.add_argument(
        "--department",
        "--dept",
        dest="dept_pattern",
        metavar="PATTERN",
        help="Filter departments using wildcard pattern.",
    )

    parser.add_argument(
        "--pi-filter",
        dest="pi_pattern",
        metavar="PATTERN",
        help="Filter pi_* accounts using wildcard pattern.",
    )

    parser.add_argument(
        "--pn",
        dest="pn_pattern",
        metavar="PATTERN",
        help="Filter pn_* accounts using wildcard pattern.",
    )

    parser.add_argument(
        "--user",
        dest="user_pattern",
        metavar="PATTERN",
        help="Filter users using wildcard pattern.",
    )

    parser.add_argument(
        "--department-summary",
        "--dept-summary",
        dest="department_summary",
        action="store_true",
        help="Show department-centric summary.",
    )

    parser.add_argument(
        "--pi",
        action="store_true",
        help="Show detailed PI-centric summary.",
    )

    parser.add_argument(
        "--pi-compact",
        action="store_true",
        help="Show compact PI summary.",
    )

    parser.add_argument(
        "--tree",
        action="store_true",
        help="Show account hierarchy including users.",
    )

    parser.add_argument(
        "--accounts",
        action="store_true",
        help="List accounts grouped by type.",
    )

    parser.add_argument(
        "--users",
        action="store_true",
        help="List users by account.",
    )

    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate account hierarchy.",
    )

    args = parser.parse_args()

    # --------------------------------------------------------------
    # Get data
    # --------------------------------------------------------------

    output = get_sacctmgr_data()

    records = parse_sacctmgr_output(
        output
    )

    if not records:

        print(
            "No Slurm associations found."
        )

        return

    # --------------------------------------------------------------
    # Determine whether filters are active
    # --------------------------------------------------------------

    filters_active = any(
        [
            args.dept_pattern,
            args.pi_pattern,
            args.pn_pattern,
            args.user_pattern,
        ]
    )

    # --------------------------------------------------------------
    # No explicit report = show everything
    #
    # If filters are supplied, however, do NOT automatically run
    # every report. Instead show the most relevant filtered report.
    # --------------------------------------------------------------

    reports_selected = any(
        [
            args.summary,
            args.department_summary,
            args.pi,
            args.pi_compact,
            args.tree,
            args.accounts,
            args.users,
            args.validate,
        ]
    )

    if not reports_selected:

        if filters_active:

            # A filter by itself means "list matching accounts/users".
            if (
                args.dept_pattern
                or args.pi_pattern
                or args.pn_pattern
            ):
                print_accounts(
                    records,
                    pattern=(
                        args.dept_pattern
                        or args.pi_pattern
                        or args.pn_pattern
                    ),
                )

            if args.user_pattern:
                print_users(
                    records,
                    pattern=args.user_pattern,
                )

        else:

            # No report and no filter:
            # show everything.
            print_summary(records)

            print_pi_summary(records)

            print_pi_compact_summary(records)

            print("=" * 70)
            print("ACCOUNT TREE")
            print("=" * 70)

            print_tree(records)

            print()

            print_accounts(records)

            print_users(records)

            print_validation(records)

        return

    # --------------------------------------------------------------
    # Explicit reports
    # --------------------------------------------------------------

    if args.summary:
        print_summary(records)

    if args.department_summary:
        print_department_summary(
            records,
            pattern=args.dept_pattern,
        )

    if args.pi:
        print_pi_summary(
            records,
            pattern=args.pi_pattern,
        )

    if args.pi_compact:
        print_pi_compact_summary(
            records,
            pattern=args.pi_pattern,
        )

    if args.tree:
        print("=" * 70)
        print("ACCOUNT TREE")
        print("=" * 70)

        print_tree(records)

        print()

    if args.accounts:

        account_pattern = (
            args.dept_pattern
            or args.pi_pattern
            or args.pn_pattern
        )

        print_accounts(
            records,
            pattern=account_pattern,
        )

    if args.users:

        print_users(
            records,
            pattern=args.user_pattern,
        )

    if args.validate:
        print_validation(records)


if __name__ == "__main__":
    main()
