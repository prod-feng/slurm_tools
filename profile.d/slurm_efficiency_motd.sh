#!/bin/bash
# Display resource usage for the latest completed/failed job, or a specified job.
# Suitable for /etc/profile.d or direct execution: ./slurm_efficiency.sh -j JOBID

export SLURM_CONF="/cm/shared/apps/slurm/var/etc/slurm/slurm.conf"

_slurm_efficiency_motd() {
    local SACCT="/cm/shared/apps/slurm/current/bin/sacct"
    local SCONTROL="/cm/shared/apps/slurm/current/bin/scontrol"
    local requested_jobid="" jobid="" start_from
    local parent_line="" batch_line="" fallback_line="" extern_line=""
    local p_jobid partition node state elapsed timelimit timelimit_raw cpus req_tres alloc_tres reqmem
    local step_id step_state step_cpus step_alloc_tres tres_ave totalcpu maxrss diskread diskwrite
    local alloc_cpu alloc_mem alloc_gpu gpu_util="N/A" gpu_mem_used="N/A"
    local runtime_sec limit_sec time_usage="N/A" cpu_sec cpu_eff="N/A"
    local mem_alloc_mb mem_used_mb mem_eff="N/A" gpu_type="N/A" node_info
    local line

    [[ -x "$SACCT" ]] || { echo "ERROR: sacct not found: $SACCT" >&2; return 1; }

    while (($#)); do
        case "$1" in
            -j|--job)
                [[ -n "${2:-}" ]] || { echo "ERROR: -j/--job requires a job ID" >&2; return 1; }
                requested_jobid="$2"; shift 2 ;;
            -h|--help)
                echo "Usage: ${BASH_SOURCE[0]} [-j JOBID|--job JOBID]"
                return 0 ;;
            *)
                echo "ERROR: unknown option: $1" >&2
                echo "Usage: ${BASH_SOURCE[0]} [-j JOBID|--job JOBID]" >&2
                return 1 ;;
        esac
    done

    if [[ -n "$requested_jobid" ]]; then
        [[ "$requested_jobid" =~ ^[0-9]+$ ]] || {
            echo "ERROR: invalid Slurm job ID: $requested_jobid" >&2; return 1;
        }
        jobid="$requested_jobid"
    else
        start_from=$(date -d '14 days ago' '+%Y-%m-%d') || return 1
        # Choose by Start timestamp, not by whichever row sacct happens to print last.
        jobid=$("$SACCT" -X -n -P -u "${USER:?USER is not set}" \
            -s "COMPLETED,FAILED,CANCELLED,TIMEOUT,OUT_OF_MEMORY,NODE_FAIL,PREEMPTED,BOOT_FAIL,DEADLINE" \
            --starttime "$start_from" --endtime now --format=JobID,Start 2>/dev/null |
            awk -F'|' '$1 ~ /^[0-9]+$/ && $2 != "" {print $2 "|" $1}' |
            sort -t'|' -k1,1 | tail -n 1 | cut -d'|' -f2)
        [[ -n "$jobid" ]] || return 0
    fi

    # One accounting query obtains the parent and its steps. Parent rows often contain
    # only billing TRES; .batch usually contains the actual resource and usage records.
    local accounting
    accounting=$("$SACCT" -j "$jobid" -n -P \
        --format=JobID,Partition,NodeList,State,Elapsed,Timelimit,TimelimitRaw,AllocCPUS,ReqTRES,AllocTRES,ReqMem,TRESUsageInAve,TRESUsageInMax,TotalCPU,MaxRSS,MaxDiskRead,MaxDiskWrite 2>/dev/null) || {
        echo "ERROR: sacct query failed for job $jobid." >&2; return 1;
    }
    [[ -n "$accounting" ]] || {
        echo "ERROR: job $jobid was not found in Slurm accounting." >&2; return 1;
    }

    while IFS='|' read -r row_id row_partition row_node row_state row_elapsed row_timelimit row_timelimit_raw row_cpus row_req_tres row_alloc_tres row_reqmem row_tres_ave row_tres_max row_totalcpu row_maxrss row_diskread row_diskwrite; do
        if [[ "$row_id" == "$jobid" ]]; then
            p_jobid="$row_id"
            partition="$row_partition"; node="$row_node"; state="$row_state"
            elapsed="$row_elapsed"; timelimit="$row_timelimit"; timelimit_raw="$row_timelimit_raw"
            cpus="$row_cpus"; req_tres="$row_req_tres"; alloc_tres="$row_alloc_tres"; reqmem="$row_reqmem"
        elif [[ "$row_id" == "$jobid.batch" ]]; then
            batch_line="$row_id"
            batch_alloc_tres="$row_alloc_tres"; batch_tres_ave="$row_tres_ave"; batch_tres_max="$row_tres_max"
            batch_totalcpu="$row_totalcpu"; batch_maxrss="$row_maxrss"
            batch_diskread="$row_diskread"; batch_diskwrite="$row_diskwrite"
        elif [[ "$row_id" == "$jobid.0" ]]; then
            fallback_line="$row_id"
            zero_alloc_tres="$row_alloc_tres"; zero_tres_ave="$row_tres_ave"; zero_tres_max="$row_tres_max"
            zero_totalcpu="$row_totalcpu"; zero_maxrss="$row_maxrss"
            zero_diskread="$row_diskread"; zero_diskwrite="$row_diskwrite"
        elif [[ "$row_id" == "$jobid.extern" ]]; then
            extern_line="$row_id"
            extern_alloc_tres="$row_alloc_tres"; extern_tres_ave="$row_tres_ave"; extern_tres_max="$row_tres_max"
            extern_totalcpu="$row_totalcpu"; extern_maxrss="$row_maxrss"
            extern_diskread="$row_diskread"; extern_diskwrite="$row_diskwrite"
        fi
    done <<< "$accounting"

    [[ -n "$p_jobid" ]] || {
        echo "ERROR: parent job $jobid was not found in Slurm accounting." >&2; return 1;
    }

    # Prefer .batch usage, then .0, and use .extern only when neither exists.
    # Some interactive jobs have no .batch/.0 row and expose only .extern.
    if [[ -n "$batch_line" ]]; then
        tres_ave="$batch_tres_ave"; tres_max="$batch_tres_max"; totalcpu="$batch_totalcpu"; maxrss="$batch_maxrss"
        diskread="$batch_diskread"; diskwrite="$batch_diskwrite"
        step_alloc_tres="$batch_alloc_tres"
        usage_step="$batch_line"
    elif [[ -n "$fallback_line" ]]; then
        tres_ave="$zero_tres_ave"; tres_max="$zero_tres_max"; totalcpu="$zero_totalcpu"; maxrss="$zero_maxrss"
        diskread="$zero_diskread"; diskwrite="$zero_diskwrite"
        step_alloc_tres="$zero_alloc_tres"
        usage_step="$fallback_line"
    elif [[ -n "$extern_line" ]]; then
        tres_ave="$extern_tres_ave"; tres_max="$extern_tres_max"; totalcpu="$extern_totalcpu"; maxrss="$extern_maxrss"
        diskread="$extern_diskread"; diskwrite="$extern_diskwrite"
        step_alloc_tres="$extern_alloc_tres"
        usage_step="$extern_line"
    else
        tres_ave=""; tres_max=""; totalcpu=""; maxrss=""; diskread=""; diskwrite=""; step_alloc_tres=""
        usage_step=""
    fi

    get_tres_value() {
        local data="$1" wanted="$2" item
        local -a items
        IFS=',' read -r -a items <<< "$data"
        for item in "${items[@]}"; do
            if [[ "${item%%=*}" == "$wanted" ]]; then
                printf '%s' "${item#*=}"
                return 0
            fi
        done
    }

    # Prefer step AllocTRES because the parent row may report only billing TRES.
    alloc_cpu=$(get_tres_value "$step_alloc_tres" cpu)
    alloc_mem=$(get_tres_value "$step_alloc_tres" mem)
    alloc_gpu=$(get_tres_value "$step_alloc_tres" gres/gpu)
    [[ "$alloc_cpu" =~ ^[0-9]+$ ]] || alloc_cpu=$(get_tres_value "$alloc_tres" cpu)
    [[ "$alloc_cpu" =~ ^[0-9]+$ ]] || alloc_cpu="${cpus:-0}"
    [[ -n "$alloc_mem" ]] || alloc_mem=$(get_tres_value "$alloc_tres" mem)
    [[ -n "$alloc_mem" ]] || alloc_mem="${reqmem:-N/A}"
    [[ "$alloc_gpu" =~ ^[0-9]+([.][0-9]+)?$ ]] || alloc_gpu=$(get_tres_value "$alloc_tres" gres/gpu)
    [[ "$alloc_gpu" =~ ^[0-9]+([.][0-9]+)?$ ]] || alloc_gpu=0

    slurm_time_seconds() {
        local v="$1" d=0 h=0 m=0 s=0
        local -a a
        [[ -n "$v" && "$v" != "Unknown" && "$v" != "UNLIMITED" ]] || { printf '0'; return; }
        if [[ "$v" == *-* ]]; then d="${v%%-*}"; v="${v#*-}"; fi
        IFS=: read -r -a a <<< "$v"
        case "${#a[@]}" in
            3) h="${a[0]}"; m="${a[1]}"; s="${a[2]}" ;;
            2) m="${a[0]}"; s="${a[1]}" ;;
            1) s="${a[0]}" ;;
        esac
        awk -v d="$d" -v h="$h" -v m="$m" -v s="$s" 'BEGIN {printf "%.6f", d*86400+h*3600+m*60+s}'
    }

    memory_to_mb() {
        local v="$1" number unit
        [[ "$v" =~ ^([0-9]+([.][0-9]+)?)([KkMmGgTt]?)$ ]] || { printf '0'; return; }
        number="${BASH_REMATCH[1]}"; unit="${BASH_REMATCH[3]^^}"
        case "$unit" in
            K) awk -v n="$number" 'BEGIN{printf "%.6f",n/1024}' ;;
            M|"") awk -v n="$number" 'BEGIN{printf "%.6f",n}' ;;
            G) awk -v n="$number" 'BEGIN{printf "%.6f",n*1024}' ;;
            T) awk -v n="$number" 'BEGIN{printf "%.6f",n*1048576}' ;;
        esac
    }

    pretty_memory() {
        local v="$1" number unit
        [[ "$v" =~ ^([0-9]+([.][0-9]+)?)([KkMmGgTt]?)$ ]] || { printf '%s' "${v:-N/A}"; return; }
        number="${BASH_REMATCH[1]}"; unit="${BASH_REMATCH[3]^^}"
        case "$unit" in
            K) awk -v n="$number" 'BEGIN{printf "%.1f KB",n}' ;;
            M|"") awk -v n="$number" 'BEGIN{printf "%.1f MB",n}' ;;
            G) awk -v n="$number" 'BEGIN{printf "%.1f GB",n}' ;;
            T) awk -v n="$number" 'BEGIN{printf "%.1f TB",n}' ;;
        esac
    }

    runtime_sec=$(slurm_time_seconds "$elapsed")
    if [[ "$timelimit_raw" =~ ^[0-9]+$ ]] && (( timelimit_raw > 0 )); then
        limit_sec=$((timelimit_raw * 60))
    else
        limit_sec=$(slurm_time_seconds "$timelimit")
    fi
    if (( limit_sec > 0 )); then
        time_usage=$(awk -v r="$runtime_sec" -v l="$limit_sec" 'BEGIN{printf "%.1f",100*r/l}')
    fi

    cpu_sec=$(slurm_time_seconds "$totalcpu")
    # A reported zero is still a numeric accounting value, so its calculated
    # efficiency is 0.0%. Missing/invalid values remain N/A.
    if [[ "$alloc_cpu" =~ ^[0-9]+$ ]] && (( alloc_cpu > 0 )) &&
       awk -v r="$runtime_sec" -v c="$cpu_sec" 'BEGIN{exit !(r>0 && c>=0)}'; then
        cpu_eff=$(awk -v c="$cpu_sec" -v r="$runtime_sec" -v n="$alloc_cpu" 'BEGIN{printf "%.1f",100*c/(r*n)}')
    fi

    # MaxRSS is the preferred peak-memory field. If absent, use the maximum
    # memory TRES; do not label TRESUsageInAve as peak memory.
    if [[ -z "$maxrss" || "$maxrss" == "Unknown" || "$maxrss" == "N/A" ]]; then
        maxrss=$(get_tres_value "$tres_max" mem)
    fi
    mem_alloc_mb=$(memory_to_mb "$alloc_mem")
    mem_used_mb=$(memory_to_mb "$maxrss")
    if awk -v a="$mem_alloc_mb" -v u="$mem_used_mb" 'BEGIN{exit !(a>0 && u>=0)}'; then
        mem_eff=$(awk -v a="$mem_alloc_mb" -v u="$mem_used_mb" 'BEGIN{printf "%.1f",100*u/a}')
    fi

    if [[ "$alloc_gpu" != "0" ]]; then
        local raw_gpu_util raw_gpu_mem
        raw_gpu_util=$(get_tres_value "$tres_ave" gres/gpuutil)
        raw_gpu_mem=$(get_tres_value "$tres_ave" gres/gpumem)
        # Slurm may aggregate GPU utilization across allocated GPUs; normalize to per-GPU %.
        if [[ "$raw_gpu_util" =~ ^[0-9]+([.][0-9]+)?$ ]] &&
           awk -v n="$alloc_gpu" 'BEGIN{exit !(n>0)}'; then
            gpu_util=$(awk -v u="$raw_gpu_util" -v n="$alloc_gpu" 'BEGIN{printf "%.1f",u/n}')
        fi
        # A numeric zero is a recorded value and should display as 0.0 MB;
        # only missing or non-numeric accounting values remain N/A.
        if [[ "$raw_gpu_mem" =~ ^[0-9]+([.][0-9]+)?([KkMmGgTt]?)$ ]]; then
            gpu_mem_used="$raw_gpu_mem"
        fi
        if [[ -n "$node" && -x "$SCONTROL" ]]; then
            node_info=$("$SCONTROL" show node "$node" 2>/dev/null)
            if [[ "$node_info" =~ Gres=gpu:([^:,[:space:]]+) ]]; then
                case "${BASH_REMATCH[1]}" in
                    6000) gpu_type="RTX PRO 6000" ;;
                    *) gpu_type="${BASH_REMATCH[1]}" ;;
                esac
            fi
        fi
    fi

    echo
    echo "╭─ Last Slurm job ─────────────────────────────────"
    echo "│ Job:       $jobid"
    echo "│ Partition: ${partition:-N/A}"
    echo "│ State:     ${state:-N/A}"
    echo "│"
    echo "│ Time"
    echo "│   Limit:        ${timelimit:-N/A}"
    echo "│   Runtime:      ${elapsed:-N/A}"
    echo "│   Usage:        ${time_usage}%"
    echo "│"
    echo "│ CPU"
    echo "│   Allocated:    ${alloc_cpu:-N/A} core(s)"
    echo "│   Used:         ${totalcpu:-N/A}"
    echo "│   Efficiency:   ${cpu_eff}%"
    echo "│"
    echo "│ Memory"
    echo "│   Allocated:    ${alloc_mem:-N/A}"
    echo "│   Peak:         $(pretty_memory "$maxrss")"
    echo "│   Efficiency:   ${mem_eff}%"
    echo "│"
    if [[ "$alloc_gpu" != "0" ]]; then
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
    echo "│ Disk I/O"
    echo "│   Read:         ${diskread:-N/A}"
    echo "│   Write:        ${diskwrite:-N/A}"
    echo "╰──────────────────────────────────────────────────"
    echo
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    _slurm_efficiency_motd "$@"
else
    _slurm_efficiency_motd
fi
unset -f _slurm_efficiency_motd
