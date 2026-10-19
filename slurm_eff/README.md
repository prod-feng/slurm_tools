# Slurm Resource Efficiency Report

A lightweight standalone Slurm resource-efficiency report generator and
HTML viewer. The tools can be used without the Slurm Analyzer Django
application or its database.

## Components

-   `standalone_resource_efficiency.py` --- collects Slurm accounting
    data, calculates resource metrics, and writes a YAML report.
-   `slurm_resource_efficiency_viewer.html` --- displays the YAML report
    in a searchable, sortable, hierarchical browser table.

## Features

### Report generator

-   Collects job accounting data using `sacct`.
-   Reads account hierarchy using
    `sacctmgr -n -p list associations tree format=Account,user,parentname`.
-   Organizes usage as **Department → PI → PN/Project → User**.
-   Reports CPU, GPU, and RAM efficiency, job counts, resource-hours,
    and resource shares.
-   Uses `TRESUsageInAve` and `TRESUsageInMax` for GPU telemetry; it
    does not depend on `GresUsageInAve` or `GresUsageInMax`.
-   Normalizes accumulated GPU-utilization telemetry by the number of
    GPUs allocated to each job before aggregating efficiency.
-   When live collection is enabled, merges live `sacctmgr` hierarchy
    mappings with a supplied account JSON file to fill missing parent
    links.
-   Warns about incomplete hierarchy mappings and retains affected usage
    under an explicit fallback hierarchy instead of silently dropping
    jobs.

### HTML viewer

-   Loads `slurm_eff.yaml` automatically when served over HTTP from the
    same directory.
-   Also supports manually selecting a `.yaml` or `.yml` file.
-   Displays expandable/collapsible department, PI, project, and user
    rows.
-   Filters by hierarchy level, department, PI, project/PN, user, or
    text search.
-   Lets you show or hide individual columns.
-   Sorts by clicking any column heading; click again to reverse the
    sort.
-   Exports the filtered rows and visible columns to CSV.
-   Runs in the browser without a Python backend or application server.
-   Loads its YAML parsing library from a CDN, so an internet connection
    is needed for that library.

## Requirements

For report generation: - Python 3. - Access to `sacct` and `sacctmgr`
for live collection. - Permission to query relevant Slurm accounting
records and account associations. - Any Python dependencies listed in
the script.

For viewing: - A modern browser such as Firefox, Chrome, or Edge. -
Internet access for the viewer's CDN-hosted YAML parser.

Check the supported script options:

``` bash
python3 standalone_resource_efficiency.py --help
```

## Quick start

### 1. Place the files together

``` text
slurm-resource-efficiency/
├── standalone_resource_efficiency.py
├── slurm_resource_efficiency_viewer.html
└── slurm_eff.yaml
```

Optional:

``` bash
chmod +x standalone_resource_efficiency.py
```

### 2. Generate a report from Slurm

Example for cluster `slurm`:

``` bash
./standalone_resource_efficiency.py \\
  --from-sacct \\
  --cluster slurm \\
  --start 2026-10-06 \\
  --end 2026-10-18 \\
  --output ./slurm_eff.yaml
```

If you also have an account hierarchy JSON file:

``` bash
./standalone_resource_efficiency.py \\
  --from-sacct \\
  --cluster slurm \\
  --start 2026-10-06 \\
  --end 2026-10-18 \\
  --accounts ./accounts.json \\
  --output ./slurm_eff.yaml
```

When `--from-sacct` is enabled, the script merges the supplied hierarchy
with live `sacctmgr` results. Adjust the cluster, dates, and file paths
for your environment. Run `--help` to see the exact options supported by
your version.

### 3. Open the viewer

**Option A --- automatic loading**

From the directory containing both the HTML and YAML files, start a
simple web server:

``` bash
python3 -m http.server 8000
```

Then open:

``` text
http://localhost:8000/slurm_resource_efficiency_viewer.html
```

The page fetches `./slurm_eff.yaml` from the same directory.

**Option B --- select the file manually**

Open `slurm_resource_efficiency_viewer.html` directly in your browser
and select `slurm_eff.yaml` in the file picker. Browsers normally
prevent pages opened via `file://` from automatically reading
neighboring files, so use the picker in this mode.

## Using the viewer

### Filters and hierarchy

The hierarchy is:

``` text
Department
└── PI
    └── PN / Project
        └── User
```

Use the dropdowns to select a department, then narrow the available PIs,
projects, and users. Use text search to find account names or IDs. The
hierarchy-level filter can focus the display on selected row types.

If account-parent mappings are missing, the report generator may retain
records under an explicit unmapped/unknown hierarchy. Those jobs remain
represented, but the true department or PI cannot be inferred without
valid parent mappings.

### Columns and sorting

-   Use **Show / hide columns** to choose visible columns.
-   Use **Show all columns** to restore the full table.
-   Click a column heading to sort; click it again to reverse direction.
-   For paired average/median efficiency columns, sorting uses the
    average value.

### CSV export

CSV export follows the active filters and visible columns, making it
convenient to analyze results in a spreadsheet.

## Understanding the metrics

Metric availability and definitions depend on the script version, Slurm
version, and accounting configuration.

-   **CPU efficiency** estimates use of allocated CPU resources.
-   **GPU efficiency** is based on Slurm TRES GPU-utilization telemetry
    and normalized by the number of GPUs allocated to each job before
    aggregation.
-   **RAM efficiency** estimates memory use relative to allocated
    memory.
-   **CPU-hours, GPU-hours, and memory GB-hours** describe resource
    allocation over time.
-   **Resource share** expresses a group's contribution to the
    corresponding report total.

These are accounting-derived estimates, not direct measures of
application performance. Telemetry may be missing or zero. Check raw
`TRESUsageInAve`/`TRESUsageInMax` values and allocated GPU counts if GPU
metrics look unexpected.

## Troubleshooting

### No departments or fewer jobs than expected

Check the script warnings about incomplete account hierarchy mappings.
Verify that `sacctmgr` returns the expected relationships:

``` bash
sacctmgr -n -p list associations tree format=Account,user,parentname
```

Confirm that `sacctmgr` is available to the account running the script
and that its hierarchy data is correct. A fallback hierarchy prevents
data loss but cannot infer missing department or PI names.

### GPU efficiency exceeds 100%

The report generator normalizes accumulated GPU-utilization telemetry by
allocated GPU count. If values still look unexpected, inspect raw TRES
usage values, allocation counts, and the local Slurm accounting
configuration.

### Automatic YAML loading fails

-   Confirm the HTML and `slurm_eff.yaml` are in the same directory.
-   Serve the directory over HTTP, for example with
    `python3 -m http.server 8000`.
-   Check that the filename is exactly `slurm_eff.yaml`.
-   If opening the HTML directly from disk, use the file picker.

### YAML parsing fails

Check that the YAML file is valid and was generated by a compatible
script version. The CDN-hosted parser must be reachable from the
browser.

## Security and privacy

Reports and CSV exports may contain usernames, account names, job
metadata, and resource usage. Handle them according to your site's data
policies. The viewer processes the selected report in the browser and
does not require uploading it to an application backend.

## License

Add the license used by your repository here.
