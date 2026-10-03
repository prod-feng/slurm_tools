#!/bin/bash

# ==============================================================
# Slurm configuration
# ==============================================================

export SLURM_CONF="/cm/shared/apps/slurm/var/etc/slurm/slurm.conf"

SACCT="/cm/shared/apps/slurm/current/bin/sacct"
SCONTROL="/cm/shared/apps/slurm/current/bin/scontrol"


# ==============================================================
# Helpers
# ==============================================================

get_tres_value()
{
    local tres="$1"
    local key="$2"

    echo "$tres" |
        tr ',' '\n' |
        awk -F= -v k="$key" '$1 == k { print $2; exit }'
}


slurm_time_seconds()
{
    local value="$1"

    [[ -z "$value" ]] && {
        echo "0"
        return
    }

    local days=0
    local hours=0
    local minutes=0
    local seconds=0

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


memory_to_mb()
{
    local value="$1"

    [[ -z "$value" ]] && {
        echo "0"
        return
    }

    local number
    local unit

    number=$(echo "$value" | sed -E 's/([0-9.]+).*/\1/')

    unit=$(
        echo "$value" |
            sed -E 's/[0-9.]+//' |
            tr '[:lower:]' '[:upper:]'
    )

    case "$unit" in

        K)
            awk -v n="$number" 'BEGIN {
                printf "%.6f", n / 1024
            }'
            ;;

        M|"")
            awk -v n="$number" 'BEGIN {
                printf "%.6f", n
            }'
            ;;

        G)
            awk -v n="$number" 'BEGIN {
                printf "%.6f", n * 1024
            }'
            ;;

        T)
            awk -v n="$number" 'BEGIN {
                printf "%.6f", n * 1024 * 1024
            }'
            ;;

        *)
            echo "0"
            ;;
    esac
}


pretty_memory()
{
    local value="$1"

    [[ -z "$value" ]] && {
        echo "N/A"
        return
    }

    local number
    local unit

    number=$(echo "$value" | sed -E 's/([0-9.]+).*/\1/')

    unit=$(
        echo "$value" |
            sed -E 's/[0-9.]+//' |
            tr '[:lower:]' '[:upper:]'
    )

    case "$unit" in

        K)
            awk -v n="$number" 'BEGIN {
                printf "%.1f KB", n
            }'
            ;;

        M)
            awk -v n="$number" 'BEGIN {
                printf "%.1f MB", n
            }'
            ;;

        G)
            awk -v n="$number" 'BEGIN {
                printf "%.1f GB", n
            }'
            ;;

        T)
            awk -v n="$number" 'BEGIN {
                printf "%.1f TB", n
            }'
            ;;

        *)
            echo "$value"
            ;;
    esac
}


gpu_display_name()
{
    local gpu="$1"

    case "$gpu" in

        6000)
            echo "RTX PRO 6000"
            ;;

        a100)
            echo "NVIDIA A100"
            ;;

        a40)
            echo "NVIDIA A40"
            ;;

        h100)
            echo "NVIDIA H100"
            ;;

        *)
            echo "$gpu"
            ;;
    esac
}


# ==============================================================
# Main
# ==============================================================

slurm_efficiency()
{
    local requested_jobid=""
    local output_format="text"


    # ----------------------------------------------------------
    # Arguments
    # ----------------------------------------------------------

    while [[ $# -gt 0 ]]; do

        case "$1" in

            -j|--job)

                [[ -z "${2:-}" ]] && {
                    echo "ERROR: -j requires a job ID" >&2
                    return 1
                }

                requested_jobid="$2"
                shift 2
                ;;

            --format)

                [[ -z "${2:-}" ]] && {
                    echo "ERROR: --format requires a value" >&2
                    return 1
                }

                output_format="$2"
                shift 2
                ;;

            --format=*)

                output_format="${1#*=}"
                shift
                ;;

            -h|--help)

                cat <<EOF
Usage:

  slurm_efficiency
  slurm_efficiency -j JOBID
  slurm_efficiency --job JOBID

Output:

  --format=text
  --format=keyvalue

Examples:

  slurm_efficiency

  slurm_efficiency -j 48260

  slurm_efficiency -j 48260 --format=keyvalue
EOF

                return 0
                ;;

            *)

                echo "ERROR: unknown option: $1" >&2
                return 1
                ;;

        esac

    done


    # ----------------------------------------------------------
    # Validate output format
    # ----------------------------------------------------------

    case "$output_format" in

        text|keyvalue)
            ;;

        *)
            echo "ERROR: unsupported format: $output_format" >&2
            return 1
            ;;

    esac


    # ----------------------------------------------------------
    # Check sacct
    # ----------------------------------------------------------

    [[ -x "$SACCT" ]] || {
        echo "ERROR: sacct not found: $SACCT" >&2
        return 1
    }


    # ----------------------------------------------------------
    # Determine job
    # ----------------------------------------------------------

    local jobid

    if [[ -n "$requested_jobid" ]]; then

        [[ "$requested_jobid" =~ ^[0-9]+$ ]] || {
            echo "ERROR: invalid job ID: $requested_jobid" >&2
            return 1
        }

        jobid="$requested_jobid"

    else

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


    [[ -n "$parent" ]] || {
        echo "ERROR: job $jobid not found." >&2
        return 1
    }


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

    alloc_cpu=$(get_tres_value "$p_alloc_tres" "cpu")
    alloc_mem=$(get_tres_value "$p_alloc_tres" "mem")
    alloc_gpu=$(get_tres_value "$p_alloc_tres" "gres/gpu")

    [[ -z "$alloc_cpu" ]] && alloc_cpu="${p_cpus:-0}"
    [[ -z "$alloc_mem" ]] && alloc_mem="${p_reqmem:-N/A}"
    [[ -z "$alloc_gpu" ]] && alloc_gpu="0"


    # ==========================================================
    # Time efficiency
    # ==========================================================

    local runtime_sec
    local limit_sec
    local time_efficiency

    runtime_sec=$(slurm_time_seconds "$p_elapsed")

    if [[ "$p_timelimit_raw" =~ ^[0-9]+$ ]] &&
       (( p_timelimit_raw > 0 )); then

        limit_sec=$((p_timelimit_raw * 60))

    else

        limit_sec=$(slurm_time_seconds "$p_timelimit")

    fi


    if (( limit_sec > 0 )); then

        time_efficiency=$(
            awk \
                -v runtime="$runtime_sec" \
                -v limit="$limit_sec" \
                'BEGIN {
                    printf "%.1f", 100 * runtime / limit
                }'
        )

    else

        time_efficiency="N/A"

    fi


    # ==========================================================
    # CPU efficiency
    # ==========================================================

    local cpu_sec
    local cpu_efficiency

    cpu_sec=$(slurm_time_seconds "$s_totalcpu")

    if (( alloc_cpu > 0 )) &&
       (( $(awk "BEGIN {print ($runtime_sec > 0)}") )) &&
       (( $(awk "BEGIN {print ($cpu_sec > 0)}") )); then

        cpu_efficiency=$(
            awk \
                -v cpu="$cpu_sec" \
                -v elapsed="$runtime_sec" \
                -v cpus="$alloc_cpu" \
                'BEGIN {
                    printf "%.1f", 100 * cpu / (elapsed * cpus)
                }'
        )

    else

        cpu_efficiency="N/A"

    fi


    # ==========================================================
    # Memory efficiency
    # ==========================================================

    local mem_alloc_mb
    local mem_used_mb
    local memory_efficiency

    mem_alloc_mb=$(memory_to_mb "$alloc_mem")
    mem_used_mb=$(memory_to_mb "$s_maxrss")

    if awk "BEGIN {
        exit !($mem_alloc_mb > 0 && $mem_used_mb > 0)
    }"; then

        memory_efficiency=$(
            awk \
                -v used="$mem_used_mb" \
                -v allocated="$mem_alloc_mb" \
                'BEGIN {
                    printf "%.2f", 100 * used / allocated
                }'
        )

    else

        memory_efficiency="N/A"

    fi


    # ==========================================================
    # GPU
    # ==========================================================

    local gpu_utilization="N/A"
    local gpu_memory_used="N/A"
    local gpu_type="N/A"

    if (( alloc_gpu > 0 )); then

        gpu_utilization=$(get_tres_value "$tres_ave" "gres/gpuutil")
        gpu_memory_used=$(get_tres_value "$tres_ave" "gres/gpumem")

        [[ -z "$gpu_utilization" ]] &&
            gpu_utilization="N/A"

        [[ -z "$gpu_memory_used" ]] &&
            gpu_memory_used="N/A"


        # ------------------------------------------------------
        # Determine GPU type from node
        # ------------------------------------------------------

        if [[ -n "$node" ]] &&
           [[ -x "$SCONTROL" ]]; then

            local node_info
            local gpu_gres_type

            node_info=$(
                "$SCONTROL" show node "$node" 2>/dev/null
            )

            gpu_gres_type=$(
                echo "$node_info" |
                    grep -oE 'Gres=gpu:[^ ]+' |
                    head -n 1 |
                    sed -E 's/Gres=gpu:([^: ]+).*/\1/'
            )

            if [[ -n "$gpu_gres_type" ]]; then
                gpu_type=$(gpu_display_name "$gpu_gres_type")
            fi

        fi

    fi


    # ==========================================================
    # KEY/VALUE OUTPUT
    # ==========================================================

    if [[ "$output_format" == "keyvalue" ]]; then

        printf 'jobid=%s\n' "$jobid"
        printf 'partition=%s\n' "${partition:-N/A}"
        printf 'node=%s\n' "${node:-N/A}"
        printf 'state=%s\n' "${p_state:-N/A}"

        printf 'runtime=%s\n' "${p_elapsed:-N/A}"
        printf 'timelimit=%s\n' "${p_timelimit:-N/A}"
        printf 'time_efficiency=%s\n' "$time_efficiency"

        printf 'cpu_allocated=%s\n' "$alloc_cpu"
        printf 'cpu_used=%s\n' "${s_totalcpu:-N/A}"
        printf 'cpu_efficiency=%s\n' "$cpu_efficiency"

        printf 'memory_allocated=%s\n' "$alloc_mem"
        printf 'memory_peak=%s\n' "${s_maxrss:-N/A}"
        printf 'memory_efficiency=%s\n' "$memory_efficiency"

        printf 'gpu_type=%s\n' "$gpu_type"
        printf 'gpu_allocated=%s\n' "$alloc_gpu"
        printf 'gpu_utilization=%s\n' "$gpu_utilization"
        printf 'gpu_memory_used=%s\n' "$gpu_memory_used"

        printf 'disk_read=%s\n' "${s_diskread:-N/A}"
        printf 'disk_write=%s\n' "${s_diskwrite:-N/A}"

        return 0
    fi


    # ==========================================================
    # TEXT OUTPUT
    # ==========================================================

    echo
    echo "╭─ Last Slurm job ─────────────────────────────────"
    echo "│ Job:       $jobid"
    echo "│ Partition: ${partition:-N/A}"
    echo "│ State:     ${p_state:-N/A}"
    echo "│"

    echo "│ Time"
    echo "│   Time limit:   ${p_timelimit:-N/A}"
    echo "│   Runtime:      ${p_elapsed:-N/A}"
    echo "│   Usage:        ${time_efficiency}%"
    echo "│"

    echo "│ CPU"
    echo "│   Allocated:    ${alloc_cpu} core(s)"
    echo "│   Used:         ${s_totalcpu:-N/A}"
    echo "│   Efficiency:   ${cpu_efficiency}%"
    echo "│"

    echo "│ Memory"
    echo "│   Allocated:    ${alloc_mem:-N/A}"
    echo "│   Peak:         $(pretty_memory "$s_maxrss")"
    echo "│   Efficiency:   ${memory_efficiency}%"
    echo "│"

    if (( alloc_gpu > 0 )); then

        echo "│ GPU"
        echo "│   Type:         ${gpu_type}"
        echo "│   Allocated:    ${alloc_gpu}"
        echo "│   Utilization:  ${gpu_utilization}%"
        echo "│   Memory used:  ${gpu_memory_used}"
        echo "│"

    fi

    echo "│ Disk I/O"
    echo "│   Read:         ${s_diskread:-N/A}"
    echo "│   Write:        ${s_diskwrite:-N/A}"
    echo "╰──────────────────────────────────────────────────"
    echo
}


# ==============================================================
# Entry point
# ==============================================================

slurm_efficiency "$@"
