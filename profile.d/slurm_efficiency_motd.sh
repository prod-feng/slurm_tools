#!/bin/bash

# ==============================================================
# Slurm efficiency MOTD
# ==============================================================
#
# Intended for /etc/profile.d/.
#
# Shows the most recent Slurm job for the login user:
#   - Time limit / runtime
#   - CPU allocation / CPU efficiency
#   - Memory allocation / peak memory / memory efficiency
#   - GPU allocation / utilization / GPU memory
#   - Disk I/O when Slurm reports it
#
# Can also be executed directly:
#   ./slurm_efficiency.sh
#   ./slurm_efficiency.sh -j 52770
#
# When sourced from /etc/profile.d, command-line arguments are NOT
# passed to the function.
# ==============================================================

export SLURM_CONF="/cm/shared/apps/slurm/var/etc/slurm/slurm.conf"

_slurm_efficiency_motd() {

    # ----------------------------------------------------------
    # Slurm commands
    # ----------------------------------------------------------

    local SACCT="/cm/shared/apps/slurm/current/bin/sacct"
    local SCONTROL="/cm/shared/apps/slurm/current/bin/scontrol"

    [[ -x "$SACCT" ]] || {
        echo "Slurm accounting command not found: $SACCT" >&2
        return 1
    }

    # ----------------------------------------------------------
    # Command-line arguments
    # ----------------------------------------------------------

    local requested_jobid=""

    while [[ $# -gt 0 ]]; do
        case "$1" in
            -j|--job)
                if [[ -z "${2:-}" ]]; then
                    echo "ERROR: -j/--job requires a job ID" >&2
                    return 1
                fi
                requested_jobid="$2"
                shift 2
                ;;
            -h|--help)
                echo "Usage: $0 [-j JOBID|--job JOBID]"
                echo
                echo "Without -j:"
                echo "  Show the most recent Slurm job."
                echo
                echo "With -j:"
                echo "  Show information for the specified job."
                echo
                echo "Examples:"
                echo "  $0"
                echo "  $0 -j 52770"
                echo "  $0 --job 48260"
                return 0
                ;;
            *)
                echo "ERROR: unknown option: $1" >&2
                echo "Usage: $0 [-j JOBID|--job JOBID]" >&2
                return 1
                ;;
        esac
    done

    # ==========================================================
    # Helper functions
    # ==========================================================

    # ----------------------------------------------------------
    # Get a value from a TRES string.
    #
    # Example:
    #   cpu=1,gres/gpu=1,mem=100G,node=1
    #
    # get_tres_value "$string" "mem"
    # ----------------------------------------------------------

    get_tres_value() {
        local tres="$1"
        local key="$2"

        printf '%s\n' "$tres" |
            tr ',' '\n' |
            awk -F= -v k="$key" '$1 == k { print $2; exit }'
    }

    # ----------------------------------------------------------
    # Convert Slurm time to seconds.
    #
    # Supports:
    #   00:02:55
    #   00:00.142
    #   1-02:03:04
    # ----------------------------------------------------------

    pretty_duration() {
        local total_seconds="${1:-0}"
        if ! [[ "$total_seconds" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
            printf 'N/A'
            return
        fi

        awk -v s="$total_seconds" 'BEGIN {
            total = int(s + 0.5)
            d = int(total / 86400)
            total %= 86400
            h = int(total / 3600)
            total %= 3600
            m = int(total / 60)
            sec = total % 60

            if (d > 0)
                printf "%dd %02d:%02d:%02d", d, h, m, sec
            else
                printf "%02d:%02d:%02d", h, m, sec
        }'
    }

    slurm_time_seconds() {
        local value="$1"
        local days=0
        local hours=0
        local minutes=0
        local seconds=0

        [[ -z "$value" ]] && {
            echo "0"
            return
        }

        if [[ "$value" == *-* ]]; then
            days="${value%%-*}"
            value="${value#*-}"
        fi

        IFS=: read -ra parts <<< "$value"

        if (( ${#parts[@]} == 3 )); then
            hours="${parts[0]}"
            minutes="${parts[1]}"
            seconds="${parts[2]}"
        elif (( ${#parts[@]} == 2 )); then
            minutes="${parts[0]}"
            seconds="${parts[1]}"
        else
            seconds="${parts[0]}"
        fi

        awk \
            -v d="$days" \
            -v h="$hours" \
            -v m="$minutes" \
            -v s="$seconds" \
            'BEGIN {
                printf "%.6f\n", d*86400+h*3600+m*60+s
            }'
    }

    # ----------------------------------------------------------
    # Convert memory to MB.
    #
    # Slurm accounting commonly reports K/M/G/T suffixes.
    # Uses 1024-based units.
    # ----------------------------------------------------------

    memory_to_mb() {
        local value="$1"
        local number
        local unit

        [[ -z "$value" || "$value" == "N/A" || "$value" == "-" ]] && {
            echo "0"
            return
        }

        number=$(printf '%s\n' "$value" |
            sed -E 's/^([0-9]+([.][0-9]+)?).*/\1/')

        unit=$(printf '%s\n' "$value" |
            sed -E 's/^[0-9]+([.][0-9]+)?//' |
            tr '[:lower:]' '[:upper:]')

        case "$unit" in
            K)
                awk -v n="$number" 'BEGIN { printf "%.6f", n / 1024 }'
                ;;
            M|"")
                awk -v n="$number" 'BEGIN { printf "%.6f", n }'
                ;;
            G)
                awk -v n="$number" 'BEGIN { printf "%.6f", n * 1024 }'
                ;;
            T)
                awk -v n="$number" 'BEGIN { printf "%.6f", n * 1024 * 1024 }'
                ;;
            *)
                echo "0"
                ;;
        esac
    }

    # ----------------------------------------------------------
    # Pretty-print memory.
    #
    # Automatically promotes units:
    #   569260K -> 556.0 MB
    #   1024M   -> 1.0 GB
    #   64G     -> 64.0 GB
    # ----------------------------------------------------------

    pretty_memory() {
        local value="$1"
        local mb

        [[ -z "$value" || "$value" == "N/A" || "$value" == "-" ]] && {
            echo "N/A"
            return
        }

        mb=$(memory_to_mb "$value")

        awk -v mb="$mb" '
            BEGIN {
                if (mb >= 1024*1024)
                    printf "%.1f TB", mb / (1024*1024)
                else if (mb >= 1024)
                    printf "%.1f GB", mb / 1024
                else if (mb >= 1)
                    printf "%.1f MB", mb
                else
                    printf "%.1f KB", mb * 1024
            }
        '
    }

    # ----------------------------------------------------------
    # Select the useful accounting step.
    #
    # Slurm does NOT guarantee that every job has a .0 step.
    # Depending on how the job was launched, accounting may show:
    #
    #   JOBID.batch
    #   JOBID.extern
    #   JOBID.0
    #   JOBID.1 ...
    #
    # Prefer .batch because it contains the resource accounting
    # for the batch script in jobs such as:
    #
    #   52770
    #   52770.batch
    #   52770.extern
    #
    # If .batch has no useful CPU/memory data, fall back to .0,
    # then to the first non-extern step.
    # ----------------------------------------------------------

    select_usage_step() {
        local jobid="$1"

        local rows
        local selected

        rows=$(
            "$SACCT" \
                -j "$jobid" \
                -n \
                -P \
                --format=JobID,State,AllocCPUS,TRESUsageInAve,TRESUsageInMax,TRESUsageInTot,TotalCPU,MaxRSS,MaxDiskRead,MaxDiskWrite 2>/dev/null
        )

        selected=$(
            printf '%s\n' "$rows" |
                awk -F'|' -v job="$jobid" '
                    $1 == job ".batch" {
                        batch=$0
                    }
                    $1 == job ".0" {
                        zero=$0
                    }
                    $1 != "" &&
                    $1 !~ /^[0-9]+\.extern$/ &&
                    $1 != job &&
                    fallback == "" {
                        fallback=$0
                    }
                    END {
                        if (batch != "")
                            print batch
                        else if (zero != "")
                            print zero
                        else if (fallback != "")
                            print fallback
                    }
                '
        )

        printf '%s\n' "$selected"
    }

    # ==========================================================
    # Find job
    # ==========================================================

    local jobid

    if [[ -n "$requested_jobid" ]]; then

        if [[ ! "$requested_jobid" =~ ^[0-9]+$ ]]; then
            echo "ERROR: invalid Slurm job ID: $requested_jobid" >&2
            return 1
        fi

        jobid="$requested_jobid"

    else

        # Find most recent job for this user.
        # Do not restrict to COMPLETED: TIMEOUT, CANCELLED, FAILED,
        # etc. should also be reported.

        # Do not use -s here.  sacct changes its default time window when
        # -s/--state is supplied, and state filters can accidentally hide
        # jobs such as TIMEOUT depending on the Slurm version/configuration.
        # Instead, request all jobs in the last 7 days and take the latest
        # top-level job record.  -X excludes job steps.
        local recent_jobs
        recent_jobs=$(
            "$SACCT" \
                -X \
                -n \
                -P \
                -u "$USER" \
                --starttime "$(date -d '7 days ago' '+%Y-%m-%d')" \
                --format=JobID,Start,State 2>/dev/null
        )

        if [[ $? -ne 0 ]]; then
            echo "ERROR: sacct failed while looking for recent jobs." >&2
            return 1
        fi

        # Keep only terminal/top-level jobs.  In particular, do not show
        # PENDING/RUNNING/etc. jobs in the login MOTD.
        # sacct State can contain annotations such as "CANCELLED by 1234",
        # so compare the first whitespace-delimited state word.
        jobid=$(
            printf '%s\n' "$recent_jobs" |
            awk -F'|' '
                $1 != "" {
                    state=$3
                    sub(/[[:space:]].*$/, "", state)
                    if (state == "COMPLETED" ||
                        state == "FAILED" ||
                        state == "CANCELLED" ||
                        state == "TIMEOUT" ||
                        state == "OUT_OF_MEMORY" ||
                        state == "NODE_FAIL" ||
                        state == "PREEMPTED" ||
                        state == "BOOT_FAIL" ||
                        state == "DEADLINE" ||
                        state == "REVOKED" ||
                        state == "SPECIAL_EXIT" ||
                        state == "SIGNALING" ||
                        state == "REQUEUED" ||
                        state == "REQUEUED_FED" ||
                        state == "REVOKED_FED" ||
                        state == "NF") {
                        print $1
                    }
                }
            ' |
            tail -n 1
        )

        if [[ -z "$jobid" ]]; then
            echo "No Slurm jobs found for $USER in the last 7 days."
            return 0
        fi
    fi

    # ==========================================================
    # Parent job
    # ==========================================================

    local parent

    parent=$(
        "$SACCT" \
            -j "$jobid" \
            -X \
            -n \
            -P \
            --format=JobID,Partition,NodeList,State,Elapsed,Timelimit,TimelimitRaw,AllocCPUS,AllocTRES,ReqMem 2>/dev/null
    )

    if [[ -z "$parent" ]]; then
        echo "ERROR: job $jobid was not found in Slurm accounting, or sacct returned no data." >&2
        return 1
    fi

    # ----------------------------------------------------------
    # Parse parent job
    # ----------------------------------------------------------

    local p_jobid
    local partition
    local node
    local p_state
    local p_elapsed
    local p_timelimit
    local p_timelimit_raw
    local p_cpus
    local p_alloc_tres
    local p_reqmem

    IFS='|' read -r \
        p_jobid \
        partition \
        node \
        p_state \
        p_elapsed \
        p_timelimit \
        p_timelimit_raw \
        p_cpus \
        p_alloc_tres \
        p_reqmem <<< "$parent"

    # ==========================================================
    # Job step / resource usage
    # ==========================================================

    local step

    step=$(select_usage_step "$jobid")

    local s_jobid=""
    local s_state=""
    local s_cpus=""
    local tres_ave=""
    local tres_max=""
    local tres_tot=""
    local s_totalcpu=""
    local s_maxrss=""
    local s_diskread=""
    local s_diskwrite=""

    if [[ -n "$step" ]]; then
        IFS='|' read -r \
            s_jobid \
            s_state \
            s_cpus \
            tres_ave \
            tres_max \
            tres_tot \
            s_totalcpu \
            s_maxrss \
            s_diskread \
            s_diskwrite <<< "$step"
    fi

    # ==========================================================
    # Allocated resources
    # ==========================================================

    local alloc_cpu
    local alloc_mem
    local alloc_gpu

    alloc_cpu=$(get_tres_value "$p_alloc_tres" "cpu")
    alloc_mem=$(get_tres_value "$p_alloc_tres" "mem")
    alloc_gpu=$(get_tres_value "$p_alloc_tres" "gres/gpu")

    [[ -z "$alloc_cpu" ]] && alloc_cpu="${p_cpus:-0}"
    [[ -z "$alloc_mem" ]] && alloc_mem="${p_reqmem:-N/A}"
    [[ -z "$alloc_gpu" ]] && alloc_gpu="0"

    # Avoid arithmetic errors if Slurm returned an unexpected value.
    [[ "$alloc_cpu" =~ ^[0-9]+([.][0-9]+)?$ ]] || alloc_cpu=0
    [[ "$alloc_gpu" =~ ^[0-9]+([.][0-9]+)?$ ]] || alloc_gpu=0

    # ==========================================================
    # GPU metrics
    # ==========================================================

    local gpu_util="N/A"
    local gpu_mem_used="N/A"

    if (( alloc_gpu > 0 )); then
        gpu_util=$(get_tres_value "$tres_ave" "gres/gpuutil")
        gpu_mem_used=$(get_tres_value "$tres_ave" "gres/gpumem")

        [[ -z "$gpu_util" ]] && gpu_util="N/A"
        [[ -z "$gpu_mem_used" ]] && gpu_mem_used="N/A"
    fi

    # ==========================================================
    # Time usage
    #
    # Parent Elapsed / Parent Timelimit
    # ==========================================================

    local runtime_sec
    local limit_sec
    local time_usage

    runtime_sec=$(slurm_time_seconds "$p_elapsed")

    if [[ "$p_timelimit_raw" =~ ^[0-9]+$ ]] &&
       (( p_timelimit_raw > 0 )); then

        # Slurm documents TimelimitRaw as minutes.
        limit_sec=$(awk -v m="$p_timelimit_raw" \
            'BEGIN { printf "%.0f", m * 60 }')
    else
        limit_sec=$(slurm_time_seconds "$p_timelimit")
    fi

    if (( limit_sec > 0 )); then
        time_usage=$(awk \
            -v runtime="$runtime_sec" \
            -v limit="$limit_sec" \
            'BEGIN {
                printf "%.1f", 100 * runtime / limit
            }')
    else
        time_usage="N/A"
    fi

    # ==========================================================
    # CPU efficiency
    #
    # CPU efficiency =
    #   CPU time / (wall runtime × allocated CPU cores)
    #
    # For job 52770:
    #   CPU time = 68.329 sec
    #   runtime  = 28816 sec
    #   CPUs     = 1
    #   efficiency ~= 0.24%
    # ==========================================================

    local cpu_sec
    local cpu_eff
    local cpu_capacity_sec
    local cpu_capacity

    cpu_sec=$(slurm_time_seconds "$s_totalcpu")

    if (( alloc_cpu > 0 )) &&
       awk "BEGIN { exit !($runtime_sec > 0) }" &&
       awk "BEGIN { exit !($cpu_sec > 0) }"; then

        # CPU capacity is the total CPU time that could have been consumed:
        # wall-clock runtime × allocated CPU cores.
        cpu_capacity_sec=$(awk \
            -v elapsed="$runtime_sec" \
            -v cpus="$alloc_cpu" \
            'BEGIN { printf "%.3f", elapsed * cpus }')

        cpu_eff=$(awk \
            -v cpu="$cpu_sec" \
            -v capacity="$cpu_capacity_sec" \
            'BEGIN {
                printf "%.2f", 100 * cpu / capacity
            }')

        cpu_capacity=$(pretty_duration "$cpu_capacity_sec")
    else
        cpu_eff="N/A"
        cpu_capacity="N/A"
    fi

    # ==========================================================
    # Memory efficiency
    # ==========================================================

    local mem_alloc_mb
    local mem_used_mb
    local mem_eff

    mem_alloc_mb=$(memory_to_mb "$alloc_mem")
    mem_used_mb=$(memory_to_mb "$s_maxrss")

    if awk "BEGIN {
        exit !($mem_alloc_mb > 0 && $mem_used_mb > 0)
    }"; then

        mem_eff=$(awk \
            -v used="$mem_used_mb" \
            -v allocated="$mem_alloc_mb" \
            'BEGIN {
                printf "%.2f", 100 * used / allocated
            }')
    else
        mem_eff="N/A"
    fi

    # ==========================================================
    # GPU type
    # ==========================================================

    local gpu_type="N/A"

    if (( alloc_gpu > 0 )) &&
       [[ -n "$node" ]] &&
       [[ -x "$SCONTROL" ]]; then

        local node_info

        node_info=$(
            "$SCONTROL" show node "$node" 2>/dev/null
        )

        gpu_type=$(
            printf '%s\n' "$node_info" |
                grep -oE 'Gres=gpu:[^ ]+' |
                head -n 1 |
                sed -E 's/Gres=gpu:([^: ]+).*/\1/'
        )

        case "$gpu_type" in
            6000)
                gpu_type="RTX PRO 6000"
                ;;
        esac

        [[ -z "$gpu_type" ]] && gpu_type="N/A"
    fi

    # ==========================================================
    # Output
    # ==========================================================

    echo
    echo "╭─ Last Slurm job ─────────────────────────────────"
    echo "│ Job:       $jobid"
    echo "│ Partition: ${partition:-N/A}"
    echo "│ State:     ${p_state:-N/A}"
    echo "│"

    # ----------------------------------------------------------
    # Time
    # ----------------------------------------------------------

    echo "│ Time"
    echo "│   Limit:        ${p_timelimit:-N/A}"
    echo "│   Runtime:      ${p_elapsed:-N/A}"
    echo "│   Usage:        ${time_usage}%"
    echo "│"

    # ----------------------------------------------------------
    # CPU
    # ----------------------------------------------------------

    echo "│ CPU"
    echo "│   Allocated:    ${alloc_cpu} core(s)"
    echo "│   Runtime:      ${p_elapsed:-N/A}"
    echo "│   CPU time:     ${s_totalcpu:-N/A}"
    echo "│   CPU capacity: ${cpu_capacity}"
    echo "│   Efficiency:   ${cpu_eff}%"
    echo "│"

    # ----------------------------------------------------------
    # Memory
    # ----------------------------------------------------------

    echo "│ Memory"
    echo "│   Allocated:    ${alloc_mem:-N/A}"
    echo "│   Peak:         $(pretty_memory "$s_maxrss")"
    echo "│   Efficiency:   ${mem_eff}%"
    echo "│"

    # ----------------------------------------------------------
    # GPU
    # ----------------------------------------------------------

    if (( alloc_gpu > 0 )); then
        echo "│ GPU"
        echo "│   Type:         ${gpu_type}"
        echo "│   Allocated:    ${alloc_gpu}"
        echo "│   Utilization:  ${gpu_util}%"
        echo "│   Memory used:  $(pretty_memory "$gpu_mem_used")"
        echo "│"
    else
        echo "│ GPU"
        echo "│   Allocated:    0"
        echo "│"
    fi

    # ----------------------------------------------------------
    # Disk I/O
    # ----------------------------------------------------------

    echo "│ Disk I/O"
    echo "│   Read:         ${s_diskread:-N/A}"
    echo "│   Write:        ${s_diskwrite:-N/A}"
    echo "╰──────────────────────────────────────────────────"
    echo
}

# ==============================================================
# Invocation
# ==============================================================

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    # Executed directly.
    _slurm_efficiency_motd "$@"
else
    # Sourced by /etc/profile or /etc/profile.d.
    _slurm_efficiency_motd
fi

unset -f _slurm_efficiency_motd
