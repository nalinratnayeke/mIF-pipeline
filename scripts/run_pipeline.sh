#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Optional environment selectors. Leave blank to use the current shell environment.
INSTANSEG_NIMBUS_ENV="${INSTANSEG_NIMBUS_ENV:-}"
SPATIALDATA_ENV="${SPATIALDATA_ENV:-}"

CONFIG=""
FORCE=0
DRY_RUN=0
declare -a SLIDE_IDS=()
declare -a STAGES=()
declare -a CHUNK_INDICES=()

usage() {
  cat <<'EOF'
Usage:
  run_pipeline.sh --config CONFIG --slide SLIDE [options]
  run_pipeline.sh --config CONFIG --slides SLIDE_A,SLIDE_B [options]

Options:
  --config PATH           Pipeline YAML config.
  --slide SLIDE_ID        Target one slide. Repeat to add more slides.
  --slides A,B,C          Comma-separated slide IDs.
  --stage NAME            Stage to run. Repeat to set an explicit stage list.
  --stages A,B,C          Comma-separated stage list.
  --chunk INDEX           Nimbus chunk index. Repeat to select multiple chunks.
  --chunks A,B,C          Comma-separated Nimbus chunk indices.
  --force                 Pass --force through to supported CLI stages.
  --dry-run               Print planned commands without executing them.
  -h, --help              Show this help text.

Stage names:
  setup
  merge
  instanseg
  nimbus
  spatialdata
  qc

Default stages:
  merge,instanseg,nimbus,spatialdata,qc

Behavior:
  - all stages run per slide and write only slide-local artifacts
  - `setup` remains channel-map generation only
  - `nimbus` expects any shared normalization JSONs to have been prepared ahead of time with `mif-pipeline nimbus-prepare`
  - `--chunk` / `--chunks` apply only to the `nimbus` stage

Examples:
  scripts/run_pipeline.sh --config prototyping/prototype_v2-Crop.yaml --slide SLIDE-0329_crop_2048
  scripts/run_pipeline.sh --config prototyping/prototype_v2-Crop.yaml --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 --stage merge --stage instanseg
  scripts/run_pipeline.sh --config prototyping/prototype_v2-Crop.yaml --slides SLIDE-0329_crop_2048,SLIDE-0329_crop_2048_2 --stage nimbus --chunk 0
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

  echo "Unable to initialize conda. Either activate the environment yourself first or set a valid env only when conda is available." >&2
  exit 1
}

activate_env_if_needed() {
  local env_target="$1"
  if [[ -z "${env_target}" ]]; then
    return 0
  fi
  ensure_conda_ready
  conda activate "${env_target}"
}

stage_env() {
  local stage="$1"
  case "${stage}" in
    spatialdata)
      printf '%s\n' "${SPATIALDATA_ENV}"
      ;;
    setup|merge|instanseg|nimbus|qc)
      printf '%s\n' "${INSTANSEG_NIMBUS_ENV}"
      ;;
    *)
      printf '\n'
      ;;
  esac
}

print_command() {
  local timestamp
  local env_target="$1"
  shift
  timestamp="$(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "[pipeline] ${timestamp} env=${env_target:-current} command: $*"
}

run_python_cli() {
  local env_target="$1"
  shift
  print_command "${env_target}" python -m mif_pipeline.cli "$@"
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    return 0
  fi
  activate_env_if_needed "${env_target}"
  PYTHONPATH="${REPO_ROOT}/src${PYTHONPATH:+:${PYTHONPATH}}" \
    python -m mif_pipeline.cli "$@"
}

validate_stage() {
  local stage="$1"
  case "${stage}" in
    setup|merge|instanseg|nimbus|spatialdata|qc)
      ;;
    *)
      echo "Unknown stage: ${stage}" >&2
      usage >&2
      exit 1
      ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      CONFIG="${2:-}"
      shift 2
      ;;
    --slide)
      append_unique "${2:-}"
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
    --chunk)
      CHUNK_INDICES+=("${2:-}")
      shift 2
      ;;
    --chunks)
      parse_csv_into_array "${2:-}" CHUNK_INDICES
      shift 2
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
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
  echo "At least one slide must be provided with --slide or --slides." >&2
  usage >&2
  exit 1
fi

if [[ ${#STAGES[@]} -eq 0 ]]; then
  STAGES=(merge instanseg nimbus spatialdata qc)
fi

for stage_name in "${STAGES[@]}"; do
  validate_stage "${stage_name}"
done

if [[ ${#CHUNK_INDICES[@]} -gt 0 ]]; then
  local_stage_ok=0
  for stage_name in "${STAGES[@]}"; do
    if [[ "${stage_name}" == "nimbus" ]]; then
      local_stage_ok=1
      break
    fi
  done
  if [[ "${local_stage_ok}" -eq 0 ]]; then
    echo "--chunk/--chunks were provided but the stage list does not include 'nimbus'." >&2
    exit 1
  fi
fi

COMMON_ARGS=(--config "${CONFIG}")
if [[ "${FORCE}" -eq 1 ]]; then
  COMMON_ARGS+=(--force)
fi

for stage_name in "${STAGES[@]}"; do
  env_target="$(stage_env "${stage_name}")"
  case "${stage_name}" in
    setup|merge|instanseg|spatialdata|qc)
      for slide_id in "${SLIDE_IDS[@]}"; do
        stage_args=("${stage_name}" "${COMMON_ARGS[@]}" --slide "${slide_id}")
        if [[ "${stage_name}" == "qc" ]]; then
          stage_args=("${stage_name}" --config "${CONFIG}" --slide "${slide_id}")
        fi
        run_python_cli "${env_target}" "${stage_args[@]}"
      done
      ;;
    nimbus)
      for slide_id in "${SLIDE_IDS[@]}"; do
        stage_args=(nimbus "${COMMON_ARGS[@]}" --slide "${slide_id}")
        for chunk_index in "${CHUNK_INDICES[@]}"; do
          stage_args+=(--chunk "${chunk_index}")
        done
        run_python_cli "${env_target}" "${stage_args[@]}"
      done
      ;;
  esac
done
