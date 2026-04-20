#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER="${REPO_ROOT}/scripts/run_pipeline.sh"

# Environment selectors passed through to submitted jobs.
INSTANSEG_NIMBUS_ENV="${INSTANSEG_NIMBUS_ENV:-}"
SPATIALDATA_ENV="${SPATIALDATA_ENV:-}"

# Default SLURM resources.
SBATCH_BIN="${SBATCH_BIN:-sbatch}"
SLURM_PARTITION="${SLURM_PARTITION:-}"
SLURM_CPUS="${SLURM_CPUS:-8}"
SLURM_MEM="${SLURM_MEM:-64G}"
SLURM_TIME="${SLURM_TIME:-24:00:00}"
SLURM_GPUS="${SLURM_GPUS:-0}"

CONFIG=""
BATCH_NAME=""
LOG_ROOT=""
FORCE=0
PLAN_ONLY=0
declare -a SLIDE_IDS=()
declare -a STAGES=()

usage() {
  cat <<'EOF'
Usage:
  run_pipeline_parallel.sh --config CONFIG --slides SLIDE_A,SLIDE_B [options]

This script is the IRIS per-slide submission wrapper. It submits one SLURM job
per slide and each job runs `scripts/run_pipeline.sh` with the selected stages.

Recommended workflow:
  1. interactively run `mif-pipeline setup` to generate channel maps
  2. interactively run `mif-pipeline nimbus-prepare` across the selected slide set
  3. submit one per-slide job with this wrapper

Options:
  --config PATH           Pipeline YAML config.
  --slides A,B,C          Comma-separated slide IDs.
  --slide SLIDE_ID        Repeatable slide ID alias for --slides.
  --stage NAME            Stage to run. Repeat to set an explicit stage list.
  --stages A,B,C          Comma-separated stage list.
  --partition NAME        SLURM partition.
  --cpus N                CPUs per submitted slide job.
  --mem MEM               Memory per submitted slide job.
  --time HH:MM:SS         Wall time per submitted slide job.
  --gpus N                GPUs per submitted slide job (default: 0).
  --batch-name NAME       Optional batch name for <log-root>/<batch-name>/.
  --log-root PATH         Parent folder for SLURM logs and manifest.
  --force                 Pass --force through to run_pipeline.sh jobs where supported.
  --plan-only             Print sbatch commands and write a planned manifest, but do not submit.
  -h, --help              Show this help text.

Stage names:
  merge
  instanseg
  nimbus
  spatialdata
  qc

Default stages:
  merge,instanseg,nimbus,spatialdata,qc

Examples:
  bash scripts/run_pipeline_parallel.sh \
    --config prototyping/prototype_v2-Crop.yaml \
    --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2

  bash scripts/run_pipeline_parallel.sh \
    --config prototyping/prototype_v2-fullslide.yaml \
    --slide SLIDE-0329 \
    --stage nimbus --stage spatialdata --stage qc
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

append_stage_unique() {
  local value="$1"
  local existing
  for existing in "${STAGES[@]}"; do
    if [[ "${existing}" == "${value}" ]]; then
      return 0
    fi
  done
  STAGES+=("${value}")
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

validate_stage() {
  local stage="$1"
  case "${stage}" in
    merge|instanseg|nimbus|spatialdata|qc)
      ;;
    *)
      echo "Unknown stage: ${stage}" >&2
      usage >&2
      exit 1
      ;;
  esac
}

shell_join() {
  local out=""
  local arg
  for arg in "$@"; do
    out+=$(printf '%q ' "${arg}")
  done
  printf '%s' "${out% }"
}

require_sbatch() {
  if [[ "${PLAN_ONLY}" -eq 1 ]]; then
    return 0
  fi
  if ! command -v "${SBATCH_BIN}" >/dev/null 2>&1; then
    echo "Could not find '${SBATCH_BIN}'. Run this wrapper on a SLURM login node or set SBATCH_BIN." >&2
    exit 1
  fi
}

LAST_SUBMITTED_JOB_ID=""

submit_job() {
  local slide_id="$1"
  shift
  local -a wrap_cmd=("$@")

  local stdout_path="${BATCH_DIR}/${slide_id}.out"
  local stderr_path="${BATCH_DIR}/${slide_id}.err"

  local -a sbatch_cmd=(
    "${SBATCH_BIN}"
    --job-name "mif_${slide_id}"
    --cpus-per-task "${SLURM_CPUS}"
    --mem "${SLURM_MEM}"
    --time "${SLURM_TIME}"
    --output "${stdout_path}"
    --error "${stderr_path}"
  )

  if [[ -n "${SLURM_PARTITION}" ]]; then
    sbatch_cmd+=(--partition "${SLURM_PARTITION}")
  fi
  if [[ "${SLURM_GPUS}" =~ ^[1-9][0-9]*$ ]]; then
    sbatch_cmd+=(--gpus "${SLURM_GPUS}")
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
    echo "[slurm] submitted ${slide_id}: ${job_id}"
  fi

  printf '%s\t%s\t%s\t%s\t%s\n' \
    "${slide_id}" \
    "${job_id}" \
    "${stdout_path}" \
    "${stderr_path}" \
    "${wrap_string}" \
    >> "${RECORDS_TSV}"
  LAST_SUBMITTED_JOB_ID="${job_id}"
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
    --stage)
      append_stage_unique "${2:-}"
      shift 2
      ;;
    --stages)
      declare -a _stages_csv=()
      parse_csv_into_array "${2:-}" _stages_csv
      for stage_name in "${_stages_csv[@]}"; do
        append_stage_unique "${stage_name}"
      done
      shift 2
      ;;
    --partition)
      SLURM_PARTITION="${2:-}"
      shift 2
      ;;
    --cpus)
      SLURM_CPUS="${2:-}"
      shift 2
      ;;
    --mem)
      SLURM_MEM="${2:-}"
      shift 2
      ;;
    --time)
      SLURM_TIME="${2:-}"
      shift 2
      ;;
    --gpus)
      SLURM_GPUS="${2:-}"
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

if [[ ${#STAGES[@]} -eq 0 ]]; then
  STAGES=(merge instanseg nimbus spatialdata qc)
fi

for stage_name in "${STAGES[@]}"; do
  validate_stage "${stage_name}"
done

if ! [[ "${SLURM_CPUS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "--cpus must be a positive integer." >&2
  exit 1
fi

if ! [[ "${SLURM_GPUS}" =~ ^[0-9]+$ ]]; then
  echo "--gpus must be a non-negative integer." >&2
  exit 1
fi

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

common_runner_args=(bash "${RUNNER}" --config "${CONFIG}")
if [[ "${FORCE}" -eq 1 ]]; then
  common_runner_args+=(--force)
fi
for stage_name in "${STAGES[@]}"; do
  common_runner_args+=(--stage "${stage_name}")
done

for slide_id in "${SLIDE_IDS[@]}"; do
  runner_cmd=("${common_runner_args[@]}" --slide "${slide_id}")
  submit_job "${slide_id}" "${runner_cmd[@]}"
done

python3 - <<'PY' \
  "${MANIFEST_PATH}" \
  "${RECORDS_TSV}" \
  "${CONFIG}" \
  "${BATCH_DIR}" \
  "${PLAN_ONLY}" \
  "${SLURM_PARTITION}" \
  "${SLURM_CPUS}" \
  "${SLURM_MEM}" \
  "${SLURM_TIME}" \
  "${SLURM_GPUS}" \
  "$(printf '%s\n' "${STAGES[@]}")" \
  "$(printf '%s\n' "${SLIDE_IDS[@]}")"
import json
import sys
from pathlib import Path

manifest_path = Path(sys.argv[1])
records_path = Path(sys.argv[2])
config_path = sys.argv[3]
batch_dir = sys.argv[4]
plan_only = bool(int(sys.argv[5]))
partition = sys.argv[6] or None
cpus = int(sys.argv[7])
mem = sys.argv[8]
time_value = sys.argv[9]
gpus = int(sys.argv[10])
stages = [value for value in sys.argv[11].splitlines() if value]
slides = [value for value in sys.argv[12].splitlines() if value]

records = []
if records_path.exists():
    for line in records_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        slide_id, job_id, stdout_path, stderr_path, command = line.split("\t", 4)
        records.append(
            {
                "slide_id": slide_id,
                "job_id": job_id or None,
                "stdout": stdout_path,
                "stderr": stderr_path,
                "command": command,
                "status": "planned" if plan_only else "submitted",
            }
        )

manifest = {
    "config": str(Path(config_path).resolve()),
    "batch_dir": batch_dir,
    "plan_only": plan_only,
    "slides": slides,
    "stages": stages,
    "resources": {
        "partition": partition,
        "cpus": cpus,
        "mem": mem,
        "time": time_value,
        "gpus": gpus,
    },
    "jobs": records,
}
manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
PY

echo "[slurm] manifest written to ${MANIFEST_PATH}"
