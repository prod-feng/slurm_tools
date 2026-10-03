#!/bin/bash

# ==============================================================
# Slurm configuration
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
    #
    #   script.sh
    #   script.sh -j 50529
    #   script.sh --job 50529
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
                echo "  $0 -j 50529"
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
    #
    # cpu=1,gres/gpu=1,mem=100G,node=1
    #
    # get_tres_value "$string" "mem"
    #
    # -> 100G
    # ----------------------------------------------------------

    get_tres_value() {

        local tres="$1"
        local key="$2"

        echo "$tres" |
            tr ',' '\n' |
            awk -F= -v k="$key" '$1 == k { print $2; exit }'
    }


    # ----------------------------------------------------------
    # Convert Slurm time to seconds.
    #
    # Supports:
    #
    #   00:02:55
    #   00:00.142
    #   1-02:03:04
    # ----------------------------------------------------------

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
    # ----------------------------------------------------------

    memory_to_mb() {

        local value="$1"

        [[ -z "$value" ]] && {
            echo "0"
            return
        }


        local number
        local unit


        number=$(
            echo "$value" |
                sed -E 's/([0-9.]+).*/\1/'
        )


        unit=$(
            echo "$value" |
                sed -E 's/[0-9.]+//' |
                tr '[:lower:]' '[:upper:]'
        )


        case "$unit" in

            K)
                awk -v n="$number" \
                    'BEGIN { printf "%.6f", n / 1024 }'
                ;;

            M|"")
                awk -v n="$number" \
                    'BEGIN { printf "%.6f", n }'
                ;;

            G)
                awk -v n="$number" \
                    'BEGIN { printf "%.6f", n * 1024 }'
                ;;

            T)
                awk -v n="$number" \
                    'BEGIN { printf "%.6f", n * 1024 * 1024 }'
                ;;

            *)
                echo "0"
                ;;

        esac
    }


    # ----------------------------------------------------------
    # Pretty-print memory.
    # ----------------------------------------------------------

    pretty_memory() {

        local value="$1"

        [[ -z "$value" ]] && {
            echo "N/A"
            return
        }


        local number
        local unit


        number=$(
            echo "$value" |
                sed -E 's/([0-9.]+).*/\1/'
        )


        unit=$(
            echo "$value" |
                sed -E 's/[0-9.]+//' |
                tr '[:lower:]' '[:upper:]'
        )


        case "$unit" in

            K)
                awk -v n="$number" \
                    'BEGIN {
                        printf "%.1f KB", n
                    }'
                ;;

            M)
                awk -v n="$number" \
                    'BEGIN {
                        printf "%.1f MB", n
                    }'
                ;;

            G)
                awk -v n="$number" \
                    'BEGIN {
                        printf "%.1f GB", n
                    }'
                ;;

            T)
                awk -v n="$number" \
                    'BEGIN {
                        printf "%.1f TB", n
                    }'
                ;;

            *)
                echo "$value"
                ;;

        esac
    }


    # ==========================================================
    # Find job
    # ==========================================================

    local jobid


    if [[ -n "$requested_jobid" ]]; then

        # Validate job ID.

        if [[ ! "$requested_jobid" =~ ^[0-9]+$ ]]; then

            echo "ERROR: invalid Slurm job ID: $requested_jobid" >&2

            return 1

        fi

        jobid="$requested_jobid"

    else

        # ------------------------------------------------------
        # Find most recent job for this user.
        #
        # Do NOT restrict to COMPLETED.
        #
        # This allows:
        #
        #   COMPLETED
        #   TIMEOUT
        #   CANCELLED
        #   FAILED
        #
        # to all be reported.
        # ------------------------------------------------------

        jobid=$(
            "$SACCT" \
                -X \
                -n \
                -P \
                -u "$USER" \
                --starttime "$(date -d '17 days ago' '+%Y-%m-%d')" \
                --format=JobID,Start |
            awk -F'|' '$1 != "" { print $1 }' |
            tail -n 1
        )


        [[ -z "$jobid" ]] && return 0

    fi


    # ==========================================================
    # Parent job
    #
    # IMPORTANT:
    #
    # Runtime and time limit come from the PARENT job.
    #
    # Do not use JOBID.0 Elapsed for walltime.
    # ==========================================================

    local parent


    parent=$(
        "$SACCT" \
            -j "$jobid" \
            -X \
            -n \
            -P \
            --format=JobID,Partition,NodeList,State,Elapsed,Timelimit,TimelimitRaw,AllocCPUS,AllocTRES,ReqMem
    )


    if [[ -z "$parent" ]]; then

        echo "ERROR: job $jobid was not found in Slurm accounting." >&2

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
    # Job step
    #
    # Use .0 for resource utilization.
    # ==========================================================

    local step


    step=$(
        "$SACCT" \
            -j "$jobid" \
            -n \
            -P \
            --format=JobID,State,AllocCPUS,TRESUsageInAve,TRESUsageInMax,TRESUsageInTot,TotalCPU,MaxRSS,MaxDiskRead,MaxDiskWrite |
        awk -F'|' '$1 == "'"${jobid}.0"'" { print; exit }'
    )


    local s_jobid
    local s_state
    local s_cpus
    local tres_ave
    local tres_max
    local tres_tot
    local s_totalcpu
    local s_maxrss
    local s_diskread
    local s_diskwrite


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


    # ==========================================================
    # Allocated resources
    # ==========================================================

    local alloc_cpu
    local alloc_mem
    local alloc_gpu


    alloc_cpu=$(
        get_tres_value "$p_alloc_tres" "cpu"
    )


    alloc_mem=$(
        get_tres_value "$p_alloc_tres" "mem"
    )


    alloc_gpu=$(
        get_tres_value "$p_alloc_tres" "gres/gpu"
    )


    [[ -z "$alloc_cpu" ]] && alloc_cpu="${p_cpus:-0}"

    [[ -z "$alloc_mem" ]] && alloc_mem="${p_reqmem:-N/A}"

    [[ -z "$alloc_gpu" ]] && alloc_gpu="0"


    # ==========================================================
    # GPU metrics
    # ==========================================================

    local gpu_util="N/A"
    local gpu_mem_used="N/A"


    if (( alloc_gpu > 0 )); then

        gpu_util=$(
            get_tres_value "$tres_ave" "gres/gpuutil"
        )


        gpu_mem_used=$(
            get_tres_value "$tres_ave" "gres/gpumem"
        )


        [[ -z "$gpu_util" ]] && gpu_util="N/A"

        [[ -z "$gpu_mem_used" ]] && gpu_mem_used="N/A"

    fi


    # ==========================================================
    # Time usage
    #
    # Parent Elapsed / Parent Timelimit
    #
    # Example:
    #
    #   Elapsed = 00:02:08
    #   Limit   = 00:02:00
    #
    #   Usage = 106.7%
    # ==========================================================

    local runtime_sec
    local limit_sec
    local time_usage


    runtime_sec=$(
        slurm_time_seconds "$p_elapsed"
    )


    if [[ "$p_timelimit_raw" =~ ^[0-9]+$ ]] &&
       (( p_timelimit_raw > 0 )); then

        # TimelimitRaw is in minutes.

        limit_sec=$(
            awk \
                -v m="$p_timelimit_raw" \
                'BEGIN { printf "%.0f", m * 60 }'
        )

    else

        limit_sec=$(
            slurm_time_seconds "$p_timelimit"
        )

    fi


    if (( limit_sec > 0 )); then

        time_usage=$(
            awk \
                -v runtime="$runtime_sec" \
                -v limit="$limit_sec" \
                'BEGIN {
                    printf "%.1f", 100 * runtime / limit
                }'
        )

    else

        time_usage="N/A"

    fi


    # ==========================================================
    # CPU efficiency
    #
    # CPU efficiency =
    #
    # CPU time / (runtime × allocated CPU cores)
    # ==========================================================

    local cpu_sec
    local cpu_eff


    cpu_sec=$(
        slurm_time_seconds "$s_totalcpu"
    )


    if (( alloc_cpu > 0 )) &&
       awk "BEGIN { exit !($runtime_sec > 0) }" &&
       awk "BEGIN { exit !($cpu_sec > 0) }"; then

        cpu_eff=$(
            awk \
                -v cpu="$cpu_sec" \
                -v elapsed="$runtime_sec" \
                -v cpus="$alloc_cpu" \
                'BEGIN {
                    printf "%.1f", 100 * cpu / (elapsed * cpus)
                }'
        )

    else

        cpu_eff="N/A"

    fi


    # ==========================================================
    # Memory efficiency
    # ==========================================================

    local mem_alloc_mb
    local mem_used_mb
    local mem_eff


    mem_alloc_mb=$(
        memory_to_mb "$alloc_mem"
    )


    mem_used_mb=$(
        memory_to_mb "$s_maxrss"
    )


    if awk "BEGIN {
        exit !($mem_alloc_mb > 0 && $mem_used_mb > 0)
    }"; then

        mem_eff=$(
            awk \
                -v used="$mem_used_mb" \
                -v allocated="$mem_alloc_mb" \
                'BEGIN {
                    printf "%.2f", 100 * used / allocated
                }'
        )

    else

        mem_eff="N/A"

    fi


    # ==========================================================
    # GPU type
    #
    # Historical job record gives us NodeList.
    #
    # scontrol show node gives:
    #
    #   Gres=gpu:6000:4
    #
    # We extract:
    #
    #   6000
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
            echo "$node_info" |
                grep -oE 'Gres=gpu:[^ ]+' |
                head -n 1 |
                sed -E 's/Gres=gpu:([^: ]+).*/\1/'
        )


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
    echo "│   Used:         ${s_totalcpu:-N/A}"
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
#
# If this file is EXECUTED:
#
#     ./slurm_efficiency.sh -j 50529
#
# pass the arguments.
#
# If this file is SOURCED by /etc/profile.d:
#
#     source /etc/profile.d/slurm_efficiency.sh
#
# do NOT pass the shell's arguments. Just show latest job.
# ==============================================================

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then

    # Executed directly.
    _slurm_efficiency_motd "$@"

else

    # Sourced by /etc/profile or /etc/profile.d.
    _slurm_efficiency_motd

fi


unset -f _slurm_efficiency_motd
