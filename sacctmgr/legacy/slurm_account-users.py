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

    A row with ParentName defines the account hierarchy.

    A row with User defines a user association.
    """

    records = []

    for line_num, line in enumerate(output.splitlines(), 1):

        line = line.strip()

        if not line:
            continue

        fields = line.split("|")

        # Make sure Account, ParentName and User exist.
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
# PI-centric analysis
# ----------------------------------------------------------------------

def get_pi_information(records):
    """
    Build PI-centric information.

    Returns:

        pi_account -> {
            department,
            projects,
            users,
            project_users
        }
    """

    accounts = get_accounts(records)
    users = get_users(records)

    pi_info = {}

    # Find all PIs.
    for account, parent in accounts.items():

        if classify_account(account) != "pi":
            continue

        pi_info[account] = {
            "department": parent,
            "projects": set(),
            "users": set(users.get(account, set())),
            "project_users": defaultdict(set),
        }

    # Find project accounts belonging to PIs.
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


def print_pi_summary(records):
    """Print detailed PI-centric report."""

    pi_info = get_pi_information(records)

    print("=" * 70)
    print("PI SUMMARY")
    print("=" * 70)

    if not pi_info:
        print("No pi_* accounts found.")
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

        print(
            f"  Department       : {department}"
        )

        print(
            f"  Project accounts : {len(projects)}"
        )

        print(
            f"  Unique users     : {len(pi_users)}"
        )

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

    print(
        f"Number of PIs       : {len(pi_info)}"
    )

    print(
        f"Project accounts    : {total_projects}"
    )

    print(
        f"Unique PI users     : {len(total_users)}"
    )

    print()


def print_pi_compact_summary(records):
    """
    Print compact PI summary with dynamically aligned columns.

    Column widths are calculated from actual data, so long
    department names do not break the alignment.
    """

    pi_info = get_pi_information(records)

    print("=" * 70)
    print("PI SUMMARY (COMPACT)")
    print("=" * 70)

    if not pi_info:
        print("No pi_* accounts found.")
        print()
        return

    # --------------------------------------------------------------
    # Prepare rows
    # --------------------------------------------------------------

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

    # --------------------------------------------------------------
    # Dynamic column widths
    # --------------------------------------------------------------

    pi_width = max(
        len("PI"),
        max(len(row[0]) for row in rows),
    )

    department_width = max(
        len("Department"),
        max(len(row[1]) for row in rows),
    )

    projects_width = max(
        len("Projects"),
        max(len(str(row[2])) for row in rows),
    )

    users_width = max(
        len("Users"),
        max(len(str(row[3])) for row in rows),
    )

    # Padding between columns.
    pi_width += 2
    department_width += 2

    # --------------------------------------------------------------
    # Header
    # --------------------------------------------------------------

    header = (
        f"{'PI':<{pi_width}}"
        f"{'Department':<{department_width}}"
        f"{'Projects':>{projects_width}}"
        f"{'Users':>{users_width}}"
    )

    print(header)
    print("-" * len(header))

    # --------------------------------------------------------------
    # Rows
    # --------------------------------------------------------------

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

            # Users associated with this account.
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

def print_accounts(records):

    accounts = get_accounts(records)
    users = get_users(records)

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

def print_users(records):

    users = get_users(records)

    print("=" * 70)
    print("USERS BY ACCOUNT")
    print("=" * 70)

    if not users:

        print("No users found.")
        print()

        return

    for account in sorted(users):

        account_users = sorted(
            users[account]
        )

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

    # --------------------------------------------------------------
    # Orphans
    # --------------------------------------------------------------

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

    # --------------------------------------------------------------
    # Hierarchy
    # --------------------------------------------------------------

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
# Main
# ----------------------------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Analyze Slurm account hierarchy, "
            "users, PIs and project accounts."
        )
    )

    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show overall account/user summary.",
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
    # Get data from Slurm
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
    # If no option was specified, show everything.
    # --------------------------------------------------------------

    show_all = not any(
        [
            args.summary,
            args.pi,
            args.pi_compact,
            args.tree,
            args.accounts,
            args.users,
            args.validate,
        ]
    )

    # --------------------------------------------------------------
    # Reports
    # --------------------------------------------------------------

    if show_all or args.summary:

        print_summary(records)

    if show_all or args.pi:

        print_pi_summary(records)

    if show_all or args.pi_compact:

        print_pi_compact_summary(
            records
        )

    if show_all or args.tree:

        print("=" * 70)
        print("ACCOUNT TREE")
        print("=" * 70)

        print_tree(records)

        print()

    if show_all or args.accounts:

        print_accounts(records)

    if show_all or args.users:

        print_users(records)

    if show_all or args.validate:

        print_validation(records)


if __name__ == "__main__":
    main()
