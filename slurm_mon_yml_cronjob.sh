#!/usr/bin/env bash
#
set -u
#
export PATH="/cm/shared/apps/slurm/current/bin:$PATH"
export LD_LIBRARY_PATH="/cm/shared/apps/slurm/current/lib64:/cm/shared/apps/slurm/current/lib64/slurm:${LD_LIBRARY_PATH:-}"
export SLURM_CONF="/cm/shared/apps/slurm/var/etc/slurm/slurm.conf"

OUTPUT="/xxx/clinwulf_stats.yaml"
TMP="${OUTPUT}.tmp"
BACKUP="${OUTPUT}.last_good"
REMOTE="xxx@xx.xx.xx.xx:/xxx/"

# Generate YAML into a temporary file
if ! /vast/projects/hpc_support/apps/clusters_yaml/slurm_mon_yml.py \
    -o "$TMP" >/dev/null 2>&1; then

    echo "ERROR: Failed to generate YAML; keeping last good file." >&2
    rm -f "$TMP"
    exit 1
fi

# Make sure the generated file is non-empty
if [[ ! -s "$TMP" ]]; then
    echo "ERROR: Generated YAML is empty; keeping last good file." >&2
    rm -f "$TMP"
    exit 1
fi

# Optional: validate that it is actually valid YAML
if ! python3 -c 'import sys, yaml; yaml.safe_load(open(sys.argv[1]))' "$TMP" \
    >/dev/null 2>&1; then

    echo "ERROR: Generated YAML is invalid; keeping last good file." >&2
    rm -f "$TMP"
    exit 1
fi

# Preserve the current good version
if [[ -f "$OUTPUT" ]]; then
    cp -p "$OUTPUT" "$BACKUP"
fi

# Atomically install the new YAML
mv -f "$TMP" "$OUTPUT"

# Copy the new YAML to the remote server
if ! scp "$OUTPUT" "$REMOTE" >/dev/null 2>&1; then
    echo "ERROR: SCP failed; rolling back to last good YAML." >&2

    if [[ -f "$BACKUP" ]]; then
        cp -p "$BACKUP" "$OUTPUT"
    fi

    exit 1
fi
