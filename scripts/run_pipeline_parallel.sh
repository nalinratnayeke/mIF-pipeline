#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER="${REPO_ROOT}/scripts/run_pipeline.sh"

# Environment selectors passed through to submitted jobs.
INSTANSEG_NIMBUS_ENV="${INSTANSEG_NIMBUS_ENV:-}"
SPATIALDATA_ENV="${SPATIALDATA_ENV:-}"

# Optional SLURM partitions per phase.
PHASE1_PARTITION="${PHASE1_PARTITION:-}"
PHASE2_PARTITION="${PHASE2_PARTITION:-}"
PHASE3_PARTITION="${PHASE3_PARTITION:-}"
PHASE4_PARTITION="${PHASE4_PARTITION:-}"
GLOBAL_PARTITION="${GLOBAL_PARTITION:-}"

# Default resource settings.
PHASE1_CPUS="${PHASE1_CPUS:-8}"
PHASE1_MEM="${PHASE1_MEM:-64G}"
PHASE1_TIME="${PHASE1_TIME:-24:00:00}"
PHASE1_GPUS="${PHASE1_GPUS:-}"

PHASE2_CPUS="${PHASE2_CPUS:-8}"
PHASE2_MEM="${PHASE2_MEM:-64G}"
PHASE2_TIME="${PHASE2_TIME:-24:00:00}"
PHASE2_GPUS="${PHASE2_GPUS:-}"

PHASE3_CPUS="${PHASE3_CPUS:-2}"
PHASE3_MEM="${PHASE3_MEM:-8G}"
PHASE3_TIME="${PHASE3_TIME:-02:00:00}"
PHASE3_GPUS="${PHASE3_GPUS:-}"

PHASE4_CPUS="${PHASE4_CPUS:-8}"
PHASE4_MEM="${PHASE4_MEM:-64G}"
PHASE4_TIME="${PHASE4_TIME:-24:00:00}"
PHASE4_GPUS="${PHASE4_GPUS:-}"
GLOBAL_GPUS="${GLOBAL_GPUS:-}"

SBATCH_BIN="${SBATCH_BIN:-sbatch}"
LAST_SUBMITTED_JOB_ID=""

CONFIG=""
PHASE="all"
BATCH_NAME=""
LOG_ROOT=""
CHUNKS_PER_JOB=1
FORCE=0
PLAN_ONLY=0
declare -a SLIDE_IDS=()
declare -a CHUNK_INDICES=()
declare -a CHUNK_GROUPS=()

usage() {
  cat <<'EOF'
Usage:
  run_pipeline_parallel.sh --config CONFIG --slides SLIDE_A,SLIDE_B [options]

This script is a SLURM submission coordinator. It runs on the login node and
submits pipeline jobs with dependencies; it does not execute pipeline stages directly.

Options:
  --config PATH           Pipeline YAML config.
  --slides A,B,C          Comma-separated slide IDs.
  --slide SLIDE_ID        Repeatable slide ID alias for --slides.
  --phase NAME            One of: all, phase1, phase2, phase3, phase4.
  --chunks A,B,C          Limit phase2 submission to these chunk indices.
  --chunk INDEX           Repeatable chunk index alias for --chunks.
  --chunks-per-job N      Group N chunk indices into each phase2 SLURM job.
  --partition NAME        Default SLURM partition for all phases.
  --phase1-partition P    Override partition for phase1.
  --phase2-partition P    Override partition for phase2.
  --phase3-partition P    Override partition for phase3.
  --phase4-partition P    Override partition for phase4.
  --gpus N                Default GPU count for all phases.
  --phase1-gpus N         Override GPU count for phase1.
  --phase2-gpus N         Override GPU count for phase2.
  --phase3-gpus N         Override GPU count for phase3.
  --phase4-gpus N         Override GPU count for phase4.
  --batch-name NAME       Optional batch name for <log-root>/<batch-name>/.
  --log-root PATH         Parent folder for SLURM logs and manifest.
  --force                 Pass --force through to run_pipeline.sh jobs where supported.
  --plan-only             Print sbatch commands and write a planned manifest, but do not submit.
  -h, --help              Show this help text.

Phases:
  all     Submit the full four-barrier job graph.
  phase1  Submit one merge+instanseg job per slide.
  phase2  Submit one multislide nimbus job per selected chunk group.
  phase3  Submit one nimbus-finalize barrier job.
  phase4  Submit one spatialdata+qc job per slide.

Examples:
  bash scripts/run_pipeline_parallel.sh \
    --config prototyping/prototype_v2-Crop.yaml \
    --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2

  bash scripts/run_pipeline_parallel.sh \
    --config prototyping/prototype_v2-Crop.yaml \
    --phase phase2 \
    --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 \
    --partition gpu \
    --phase2-gpus 1 \
    --chunks-per-job 2 \
    --chunk 0

  bash scripts/run_pipeline_parallel.sh \
    --config prototyping/prototype_v2-Crop.yaml \
    --phase all \
    --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 \
    --log-root /data/example_lab/slurm_logs \
    --plan-only
EOF
}

append_unique() {
  local value="$1"
  local existing
  for existing in "${SLIDE_IDS[@]}"; do
    if [[ "${existing}" == "${value}" ]]; then
      return 0
    fi
  done
  SLIDE_IDS+=("${value}")
}

append_chunk_unique() {
  local value="$1"
  local existing
  for existing in "${CHUNK_INDICES[@]}"; do
    if [[ "${existing}" == "${value}" ]]; then
      return 0
    fi
  done
  CHUNK_INDICES+=("${value}")
}

parse_csv_into_array() {
  local csv="$1"
  local -n target_array="$2"
  local item
  IFS=',' read -r -a _items <<< "${csv}"
  for item in "${_items[@]}"; do
    item="${item#"${item%%[![:space:]]*}"}"
    item="${item%"${item##*[![:space:]]}"}"
    if [[ -n "${item}" ]]; then
      target_array+=("${item}")
    fi
  done
}

CONDA_READY=0

ensure_conda_ready() {
  if [[ "${CONDA_READY}" -eq 1 ]]; then
    return 0
  fi

  if command -v conda >/dev/null 2>&1; then
    eval "$(conda shell.bash hook)"
    CONDA_READY=1
    return 0
  fi

  if type module >/dev/null 2>&1; then
    module load miniforge3 >/dev/null 2>&1 || true
  elif [[ -f /etc/profile.d/modules.sh ]]; then
    # shellcheck disable=SC1091
    source /etc/profile.d/modules.sh
    if type module >/dev/null 2>&1; then
      module load miniforge3 >/dev/null 2>&1 || true
    fi
  fi

  if command -v conda >/dev/null 2>&1; then
    eval "$(conda shell.bash hook)"
    CONDA_READY=1
    return 0
  fi

  local candidate
  for candidate in \
    "${HOME}/miniconda3/etc/profile.d/conda.sh" \
    "${HOME}/miniforge3/etc/profile.d/conda.sh" \
    "/home/ratnayn/miniconda3/etc/profile.d/conda.sh"
  do
    if [[ -f "${candidate}" ]]; then
      # shellcheck disable=SC1090
      source "${candidate}"
      eval "$(conda shell.bash hook)"
      CONDA_READY=1
      return 0
    fi
  done

  echo "Unable to initialize conda for config/chunk planning. Activate a suitable environment first or set INSTANSEG_NIMBUS_ENV." >&2
  exit 1
}

run_helper_python() {
  if [[ -n "${INSTANSEG_NIMBUS_ENV}" ]]; then
    ensure_conda_ready
    conda activate "${INSTANSEG_NIMBUS_ENV}"
  fi
  PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" python3 "$@"
}

shell_join() {
  local out=""
  local arg
  for arg in "$@"; do
    out+=$(printf '%q ' "${arg}")
  done
  printf '%s' "${out% }"
}

print_command() {
  echo "$(shell_join "$@")"
}

require_sbatch() {
  if [[ "${PLAN_ONLY}" -eq 1 ]]; then
    return 0
  fi
  if ! command -v "${SBATCH_BIN}" >/dev/null 2>&1; then
    echo "Could not find '${SBATCH_BIN}'. Run this coordinator on a SLURM login node or set SBATCH_BIN." >&2
    exit 1
  fi
}

phase_partition() {
  case "$1" in
    phase1) printf '%s\n' "${PHASE1_PARTITION:-${GLOBAL_PARTITION}}" ;;
    phase2) printf '%s\n' "${PHASE2_PARTITION:-${GLOBAL_PARTITION}}" ;;
    phase3) printf '%s\n' "${PHASE3_PARTITION:-${GLOBAL_PARTITION}}" ;;
    phase4) printf '%s\n' "${PHASE4_PARTITION:-${GLOBAL_PARTITION}}" ;;
  esac
}

phase_cpus() {
  case "$1" in
    phase1) printf '%s\n' "${PHASE1_CPUS}" ;;
    phase2) printf '%s\n' "${PHASE2_CPUS}" ;;
    phase3) printf '%s\n' "${PHASE3_CPUS}" ;;
    phase4) printf '%s\n' "${PHASE4_CPUS}" ;;
  esac
}

phase_mem() {
  case "$1" in
    phase1) printf '%s\n' "${PHASE1_MEM}" ;;
    phase2) printf '%s\n' "${PHASE2_MEM}" ;;
    phase3) printf '%s\n' "${PHASE3_MEM}" ;;
    phase4) printf '%s\n' "${PHASE4_MEM}" ;;
  esac
}

phase_time() {
  case "$1" in
    phase1) printf '%s\n' "${PHASE1_TIME}" ;;
    phase2) printf '%s\n' "${PHASE2_TIME}" ;;
    phase3) printf '%s\n' "${PHASE3_TIME}" ;;
    phase4) printf '%s\n' "${PHASE4_TIME}" ;;
  esac
}

phase_gpus() {
  case "$1" in
    phase1) printf '%s\n' "${PHASE1_GPUS:-${GLOBAL_GPUS:-0}}" ;;
    phase2) printf '%s\n' "${PHASE2_GPUS:-${GLOBAL_GPUS:-0}}" ;;
    phase3) printf '%s\n' "${PHASE3_GPUS:-${GLOBAL_GPUS:-0}}" ;;
    phase4) printf '%s\n' "${PHASE4_GPUS:-${GLOBAL_GPUS:-0}}" ;;
  esac
}

append_record() {
  local phase="$1"
  local target="$2"
  local job_id="$3"
  local dependency="$4"
  local stdout_path="$5"
  local stderr_path="$6"
  local command="$7"
  local status="$8"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${phase}" "${target}" "${job_id}" "${dependency}" "${stdout_path}" "${stderr_path}" "${command}" "${status}" \
    >> "${RECORDS_TSV}"
}

create_manifest() {
  run_helper_python - <<'PY' \
    "${MANIFEST_PATH}" \
    "${RECORDS_TSV}" \
    "${CONFIG}" \
    "${BATCH_DIR}" \
    "${PHASE}" \
    "${PLAN_ONLY}" \
    "${CHUNKS_PER_JOB}" \
    "${INSTANSEG_NIMBUS_ENV}" \
    "${SPATIALDATA_ENV}" \
    "${GLOBAL_PARTITION}" \
    "${GLOBAL_GPUS}" \
    "${PHASE1_PARTITION}" \
    "${PHASE2_PARTITION}" \
    "${PHASE3_PARTITION}" \
    "${PHASE4_PARTITION}" \
    "${PHASE1_GPUS}" \
    "${PHASE2_GPUS}" \
    "${PHASE3_GPUS}" \
    "${PHASE4_GPUS}" \
    "$(printf '%s\n' "${SLIDE_IDS[@]}")" \
    "$(printf '%s\n' "${SELECTED_CHUNK_INDICES[@]}")"
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
records_path = Path(sys.argv[2])
config_path = sys.argv[3]
batch_dir = sys.argv[4]
phase = sys.argv[5]
plan_only = bool(int(sys.argv[6]))
chunks_per_job = int(sys.argv[7])
inst_env = sys.argv[8]
spatial_env = sys.argv[9]
global_partition = sys.argv[10] or None
global_gpus = sys.argv[11] or None
phase1_partition = sys.argv[12] or None
phase2_partition = sys.argv[13] or None
phase3_partition = sys.argv[14] or None
phase4_partition = sys.argv[15] or None
phase1_gpus = sys.argv[16] or None
phase2_gpus = sys.argv[17] or None
phase3_gpus = sys.argv[18] or None
phase4_gpus = sys.argv[19] or None
slides = [value for value in sys.argv[20].splitlines() if value]
chunks = [int(value) for value in sys.argv[21].splitlines() if value]

records = []
with records_path.open() as handle:
    for line in handle:
        phase_name, target, job_id, dependency, stdout_path, stderr_path, command, status = line.rstrip("\n").split("\t")
        records.append(
            {
                "phase": phase_name,
                "target": target,
                "job_id": job_id or None,
                "dependency": dependency or None,
                "stdout": stdout_path,
                "stderr": stderr_path,
                "command": command,
                "status": status,
            }
        )

manifest = {
    "config_path": config_path,
    "batch_dir": batch_dir,
    "phase": phase,
    "plan_only": plan_only,
    "chunks_per_job": chunks_per_job,
    "slides": slides,
    "chunk_indices": chunks,
    "instanseg_nimbus_env": inst_env,
    "spatialdata_env": spatial_env,
    "resources": {
        "global_partition": global_partition,
        "global_gpus": global_gpus,
        "phase1_partition": phase1_partition,
        "phase2_partition": phase2_partition,
        "phase3_partition": phase3_partition,
        "phase4_partition": phase4_partition,
        "phase1_gpus": phase1_gpus,
        "phase2_gpus": phase2_gpus,
        "phase3_gpus": phase3_gpus,
        "phase4_gpus": phase4_gpus,
    },
    "records": records,
}
manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
PY
}

submit_job() {
  local phase="$1"
  local target="$2"
  local dependency="$3"
  shift 3
  local wrap_cmd=("$@")

  local job_name stdout_path stderr_path partition cpus mem time_limit gpus
  job_name="mif-${phase}-${target}"
  stdout_path="${BATCH_DIR}/${phase}-${target}.out"
  stderr_path="${BATCH_DIR}/${phase}-${target}.err"
  partition="$(phase_partition "${phase}")"
  cpus="$(phase_cpus "${phase}")"
  mem="$(phase_mem "${phase}")"
  time_limit="$(phase_time "${phase}")"
  gpus="$(phase_gpus "${phase}")"

  local sbatch_cmd=(
    "${SBATCH_BIN}"
    --parsable
    --job-name "${job_name}"
    --output "${stdout_path}"
    --error "${stderr_path}"
    --cpus-per-task "${cpus}"
    --mem "${mem}"
    --time "${time_limit}"
  )

  if [[ -n "${partition}" ]]; then
    sbatch_cmd+=(--partition "${partition}")
  fi
  if [[ -n "${gpus}" && "${gpus}" != "0" ]]; then
    sbatch_cmd+=(--gpus "${gpus}")
  fi
  if [[ -n "${dependency}" ]]; then
    sbatch_cmd+=(--dependency "afterok:${dependency}")
  fi

  local export_vars="ALL"
  if [[ -n "${INSTANSEG_NIMBUS_ENV}" ]]; then
    export_vars+=",INSTANSEG_NIMBUS_ENV=${INSTANSEG_NIMBUS_ENV}"
  fi
  if [[ -n "${SPATIALDATA_ENV}" ]]; then
    export_vars+=",SPATIALDATA_ENV=${SPATIALDATA_ENV}"
  fi
  sbatch_cmd+=(--export "${export_vars}")

  local wrap_string
  wrap_string="$(shell_join "${wrap_cmd[@]}")"
  sbatch_cmd+=(--wrap "${wrap_string}")

  local sbatch_print
  sbatch_print="$(shell_join "${sbatch_cmd[@]}")"
  echo "${sbatch_print}"

  local job_id=""
  if [[ "${PLAN_ONLY}" -eq 0 ]]; then
    job_id="$("${sbatch_cmd[@]}")"
    echo "[slurm] submitted ${phase}/${target}: ${job_id}"
  fi

  append_record "${phase}" "${target}" "${job_id}" "${dependency}" "${stdout_path}" "${stderr_path}" "${wrap_string}" "$([[ "${PLAN_ONLY}" -eq 1 ]] && printf 'planned' || printf 'submitted')"
  LAST_SUBMITTED_JOB_ID="${job_id}"
}

compute_all_chunk_indices() {
  mapfile -t all_chunk_indices < <(
    run_helper_python - <<'PY' "${CONFIG}" "$(printf '%s\n' "${SLIDE_IDS[@]}")"
from mif_pipeline.config import chunked, get_slide_config, load_config, resolve_nimbus_multislide_inputs
import sys

config = load_config(sys.argv[1])
slide_ids = [value for value in sys.argv[2].splitlines() if value]
resolved = resolve_nimbus_multislide_inputs(config, slide_ids)
slide = get_slide_config(config, resolved["slide_ids"][0])
chunk_size = int((slide.get("nimbus") or {}).get("channel_chunk_size", 1))
aliases = list(resolved["aliases"])
for index, _chunk in enumerate(chunked(aliases, chunk_size)):
    print(index)
PY
  )
}

normalize_selected_chunk_indices() {
  compute_all_chunk_indices
  if [[ ${#CHUNK_INDICES[@]} -eq 0 ]]; then
    SELECTED_CHUNK_INDICES=("${all_chunk_indices[@]}")
    return 0
  fi

  declare -A valid_map=()
  local idx
  for idx in "${all_chunk_indices[@]}"; do
    valid_map["${idx}"]=1
  done

  SELECTED_CHUNK_INDICES=()
  for idx in "${CHUNK_INDICES[@]}"; do
    if [[ -z "${valid_map[${idx}]:-}" ]]; then
      echo "Chunk index ${idx} is invalid for this config/slide set. Valid chunk indices: ${all_chunk_indices[*]}" >&2
      exit 1
    fi
    SELECTED_CHUNK_INDICES+=("${idx}")
  done
}

build_chunk_groups() {
  CHUNK_GROUPS=()
  local -a current_group=()
  local idx
  for idx in "${SELECTED_CHUNK_INDICES[@]}"; do
    current_group+=("${idx}")
    if [[ ${#current_group[@]} -ge "${CHUNKS_PER_JOB}" ]]; then
      CHUNK_GROUPS+=("$(IFS=,; printf '%s' "${current_group[*]}")")
      current_group=()
    fi
  done
  if [[ ${#current_group[@]} -gt 0 ]]; then
    CHUNK_GROUPS+=("$(IFS=,; printf '%s' "${current_group[*]}")")
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="${2:-}"
      shift 2
      ;;
    --slides)
      declare -a _slides_csv=()
      parse_csv_into_array "${2:-}" _slides_csv
      for slide_id in "${_slides_csv[@]}"; do
        append_unique "${slide_id}"
      done
      shift 2
      ;;
    --slide)
      append_unique "${2:-}"
      shift 2
      ;;
    --phase)
      PHASE="${2:-}"
      shift 2
      ;;
    --chunks)
      declare -a _chunks_csv=()
      parse_csv_into_array "${2:-}" _chunks_csv
      for chunk_idx in "${_chunks_csv[@]}"; do
        append_chunk_unique "${chunk_idx}"
      done
      shift 2
      ;;
    --chunk)
      append_chunk_unique "${2:-}"
      shift 2
      ;;
    --chunks-per-job)
      CHUNKS_PER_JOB="${2:-}"
      shift 2
      ;;
    --partition)
      GLOBAL_PARTITION="${2:-}"
      shift 2
      ;;
    --phase1-partition)
      PHASE1_PARTITION="${2:-}"
      shift 2
      ;;
    --phase2-partition)
      PHASE2_PARTITION="${2:-}"
      shift 2
      ;;
    --phase3-partition)
      PHASE3_PARTITION="${2:-}"
      shift 2
      ;;
    --phase4-partition)
      PHASE4_PARTITION="${2:-}"
      shift 2
      ;;
    --gpus)
      GLOBAL_GPUS="${2:-}"
      shift 2
      ;;
    --phase1-gpus)
      PHASE1_GPUS="${2:-}"
      shift 2
      ;;
    --phase2-gpus)
      PHASE2_GPUS="${2:-}"
      shift 2
      ;;
    --phase3-gpus)
      PHASE3_GPUS="${2:-}"
      shift 2
      ;;
    --phase4-gpus)
      PHASE4_GPUS="${2:-}"
      shift 2
      ;;
    --batch-name)
      BATCH_NAME="${2:-}"
      shift 2
      ;;
    --log-root)
      LOG_ROOT="${2:-}"
      shift 2
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --plan-only)
      PLAN_ONLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "${CONFIG}" ]]; then
  echo "--config is required." >&2
  usage >&2
  exit 1
fi

if [[ ${#SLIDE_IDS[@]} -eq 0 ]]; then
  echo "At least one slide must be provided with --slides or --slide." >&2
  usage >&2
  exit 1
fi

case "${PHASE}" in
  all|phase1|phase2|phase3|phase4)
    ;;
  *)
    echo "Unknown phase: ${PHASE}. Expected one of all, phase1, phase2, phase3, phase4." >&2
    exit 1
    ;;
esac

if ! [[ "${CHUNKS_PER_JOB}" =~ ^[1-9][0-9]*$ ]]; then
  echo "--chunks-per-job must be a positive integer." >&2
  exit 1
fi

for gpu_value in "${GLOBAL_GPUS:-}" "${PHASE1_GPUS}" "${PHASE2_GPUS}" "${PHASE3_GPUS}" "${PHASE4_GPUS}"; do
  if [[ -n "${gpu_value}" && ! "${gpu_value}" =~ ^[0-9]+$ ]]; then
    echo "GPU values must be non-negative integers." >&2
    exit 1
  fi
done

require_sbatch

if [[ -z "${BATCH_NAME}" ]]; then
  BATCH_NAME="$(date '+%Y%m%d_%H%M%S')"
fi

if [[ -z "${LOG_ROOT}" ]]; then
  LOG_ROOT="${REPO_ROOT}/logs/slurm"
fi

BATCH_DIR="${LOG_ROOT}/${BATCH_NAME}"
mkdir -p "${BATCH_DIR}"
RECORDS_TSV="${BATCH_DIR}/records.tsv"
MANIFEST_PATH="${BATCH_DIR}/manifest.json"
: > "${RECORDS_TSV}"

SELECTED_CHUNK_INDICES=()
if [[ "${PHASE}" == "all" || "${PHASE}" == "phase2" ]]; then
  normalize_selected_chunk_indices
  build_chunk_groups
fi

common_runner_args=(bash "${RUNNER}" --config "${CONFIG}")
if [[ "${FORCE}" -eq 1 ]]; then
  common_runner_args+=(--force)
fi

declare -a phase1_job_ids=()
declare -a phase2_job_ids=()
phase3_job_id=""

if [[ "${PHASE}" == "all" || "${PHASE}" == "phase1" ]]; then
  for slide_id in "${SLIDE_IDS[@]}"; do
    runner_cmd=("${common_runner_args[@]}" --slide "${slide_id}" --stage merge --stage instanseg)
    submit_job "phase1" "${slide_id}" "" "${runner_cmd[@]}"
    if [[ -n "${LAST_SUBMITTED_JOB_ID}" ]]; then
      phase1_job_ids+=("${LAST_SUBMITTED_JOB_ID}")
    fi
  done
fi

if [[ "${PHASE}" == "all" || "${PHASE}" == "phase2" ]]; then
  dependency=""
  if [[ "${PHASE}" == "all" && ${#phase1_job_ids[@]} -gt 0 ]]; then
    dependency="$(IFS=:; printf '%s' "${phase1_job_ids[*]}")"
  fi
  for chunk_group in "${CHUNK_GROUPS[@]}"; do
    runner_cmd=("${common_runner_args[@]}" --stage nimbus --chunks "${chunk_group}")
    for slide_id in "${SLIDE_IDS[@]}"; do
      runner_cmd+=(--slide "${slide_id}")
    done
    submit_job "phase2" "chunks_${chunk_group//,/__}" "${dependency}" "${runner_cmd[@]}"
    if [[ -n "${LAST_SUBMITTED_JOB_ID}" ]]; then
      phase2_job_ids+=("${LAST_SUBMITTED_JOB_ID}")
    fi
  done
fi

if [[ "${PHASE}" == "all" || "${PHASE}" == "phase3" ]]; then
  dependency=""
  if [[ "${PHASE}" == "all" && ${#phase2_job_ids[@]} -gt 0 ]]; then
    dependency="$(IFS=:; printf '%s' "${phase2_job_ids[*]}")"
  fi
  runner_cmd=("${common_runner_args[@]}" --stage nimbus-finalize)
  for slide_id in "${SLIDE_IDS[@]}"; do
    runner_cmd+=(--slide "${slide_id}")
  done
  submit_job "phase3" "finalize" "${dependency}" "${runner_cmd[@]}"
  phase3_job_id="${LAST_SUBMITTED_JOB_ID}"
fi

if [[ "${PHASE}" == "all" || "${PHASE}" == "phase4" ]]; then
  dependency=""
  if [[ "${PHASE}" == "all" && -n "${phase3_job_id}" ]]; then
    dependency="${phase3_job_id}"
  fi
  for slide_id in "${SLIDE_IDS[@]}"; do
    runner_cmd=("${common_runner_args[@]}" --slide "${slide_id}" --stage spatialdata --stage qc)
    submit_job "phase4" "${slide_id}" "${dependency}" "${runner_cmd[@]}"
  done
fi

create_manifest
echo "[slurm] manifest written to ${MANIFEST_PATH}"
