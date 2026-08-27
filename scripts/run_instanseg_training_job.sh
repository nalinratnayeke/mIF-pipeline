#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEFAULT_MATRIX="${REPO_ROOT}/training/instanseg_experiments.json"
DEFAULT_FORK_ROOT="/data1/lowes/ratnayn/Codex/projects/instanseg"
DEFAULT_PYTHON="/data1/lowes/ratnayn/conda_envs/instanseg_training/bin/python"

EXPERIMENT_NAME="${1:-}"
MATRIX_PATH="${2:-${DEFAULT_MATRIX}}"

if [[ -z "${EXPERIMENT_NAME}" ]]; then
  echo "Usage: run_instanseg_training_job.sh EXPERIMENT_NAME [MATRIX_JSON]" >&2
  exit 2
fi
if [[ ! -f "${MATRIX_PATH}" ]]; then
  echo "Missing experiment matrix: ${MATRIX_PATH}" >&2
  exit 2
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to read ${MATRIX_PATH}" >&2
  exit 2
fi

experiment_json="$(jq -ce --arg name "${EXPERIMENT_NAME}" '.experiments[] | select(.name == $name)' "${MATRIX_PATH}")" || {
  echo "Unknown experiment '${EXPERIMENT_NAME}' in ${MATRIX_PATH}" >&2
  exit 2
}
defaults_json="$(jq -ce '.defaults' "${MATRIX_PATH}")"
resolved_json="$(jq -cn \
  --argjson defaults "${defaults_json}" \
  --argjson experiment "${experiment_json}" \
  '$defaults + $experiment')"

ready="$(jq -r '.ready' <<< "${experiment_json}")"
if [[ "${ready}" != "true" && "${ALLOW_BLOCKED_EXPERIMENT:-0}" != "1" ]]; then
  reason="$(jq -r '.blocked_reason // "No reason recorded"' <<< "${experiment_json}")"
  echo "Experiment '${EXPERIMENT_NAME}' is not ready: ${reason}" >&2
  echo "Set ALLOW_BLOCKED_EXPERIMENT=1 only after resolving and documenting the blocker." >&2
  exit 2
fi

json_value() {
  local expression="$1"
  jq -r "${expression}" <<< "${resolved_json}"
}

training_root="$(jq -er '.training_root' <<< "${defaults_json}")"
dataset_name="$(jq -er '.dataset' <<< "${defaults_json}")"
source_dataset="$(json_value '.source_dataset')"
requested_pixel_size="$(json_value '.requested_pixel_size')"
target_segmentation="$(json_value '.target_segmentation')"
channel_invariant="$(json_value '.channel_invariant')"
multihead="$(json_value '.multihead // false')"
augmentation_type="$(json_value '.augmentation_type')"
weight="$(json_value '.weight')"
batch_size="$(json_value '.batch_size')"
num_workers="$(json_value '.num_workers')"
num_epochs="$(json_value '.num_epochs')"
length_of_epoch="$(json_value '.length_of_epoch')"
learning_rate="$(json_value '.learning_rate')"
tile_size="$(json_value '.tile_size')"
window_size="$(json_value '.window_size')"
hotstart_training="$(json_value '.hotstart_training')"
rng_seed="$(json_value '.rng_seed')"
channel_drop="$(jq -c '.dataset_channel_drop_probabilities' <<< "${experiment_json}")"

python_bin="${INSTANSEG_TRAINING_PYTHON:-${DEFAULT_PYTHON}}"
source_root="${INSTANSEG_SOURCE_ROOT:-${DEFAULT_FORK_ROOT}}"
dataset_root="${training_root}/datasets"
dataset_file="${dataset_root}/${dataset_name}_dataset.pth"
model_root="${training_root}/models"
experiment_output="${model_root}/${EXPERIMENT_NAME}"
resume_training="${RESUME_TRAINING:-0}"
resume_checkpoint="${experiment_output}/latest_checkpoint.pth"

if [[ ! -x "${python_bin}" ]]; then
  echo "Training Python is not executable: ${python_bin}" >&2
  exit 2
fi
if [[ ! -f "${source_root}/instanseg/__init__.py" ]]; then
  echo "InstanSeg source root is invalid: ${source_root}" >&2
  exit 2
fi
if [[ ! -f "${dataset_file}" ]]; then
  echo "Dataset checkpoint is missing: ${dataset_file}" >&2
  exit 2
fi
if [[ "${resume_training}" != "0" && "${resume_training}" != "1" ]]; then
  echo "RESUME_TRAINING must be 0 or 1: ${resume_training}" >&2
  exit 2
fi
if [[ -e "${experiment_output}" ]]; then
  if [[ "${resume_training}" != "1" ]]; then
    echo "Experiment output already exists: ${experiment_output}" >&2
    echo "Use the submission wrapper's --resume option only for an interrupted run." >&2
    exit 2
  fi
  if [[ -f "${experiment_output}/training_complete.json" ]]; then
    echo "Experiment is already marked complete: ${experiment_output}" >&2
    exit 2
  fi
  if [[ ! -f "${resume_checkpoint}" ]]; then
    echo "Cannot resume without ${resume_checkpoint}" >&2
    exit 2
  fi
elif [[ "${resume_training}" == "1" ]]; then
  echo "Cannot resume because the experiment output does not exist: ${experiment_output}" >&2
  exit 2
fi

mkdir -p "${model_root}"

export PYTHONPATH="${source_root}${PYTHONPATH:+:${PYTHONPATH}}"
export INSTANSEG_DATASET_PATH="${dataset_root}"
export INSTANSEG_OUTPUT_PATH="${model_root}"
export MPLCONFIGDIR="${TMPDIR:-/tmp}/instanseg-matplotlib-${SLURM_JOB_ID:-local}"
export XDG_CACHE_HOME="${TMPDIR:-/tmp}/instanseg-cache-${SLURM_JOB_ID:-local}"
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
mkdir -p "${MPLCONFIGDIR}" "${XDG_CACHE_HOME}"

echo "===== INSTANSEG TRAINING JOB ====="
date
hostname
echo "experiment=${EXPERIMENT_NAME}"
echo "purpose=$(jq -r '.purpose' <<< "${experiment_json}")"
echo "matrix=${MATRIX_PATH}"
echo "python=${python_bin}"
echo "source_root=${source_root}"
echo "dataset=${dataset_file}"
echo "output=${experiment_output}"
echo "resume_training=${resume_training}"
if [[ "${resume_training}" == "1" ]]; then
  echo "resume_checkpoint=${resume_checkpoint}"
fi
echo "SLURM_JOB_ID=${SLURM_JOB_ID:-}"
echo "SLURM_CPUS_PER_TASK=${SLURM_CPUS_PER_TASK:-}"
echo "CUDA_VISIBLE_DEVICES=${CUDA_VISIBLE_DEVICES:-}"
nvidia-smi || true
"${python_bin}" - <<'PY'
from pathlib import Path
import instanseg
import torch

print(f"InstanSeg import: {Path(instanseg.__file__).resolve()}")
print(f"PyTorch: {torch.__version__}")
print(f"CUDA available: {torch.cuda.is_available()}")
if torch.cuda.is_available():
    print(f"GPU: {torch.cuda.get_device_name(torch.cuda.current_device())}")
PY

command=(
  "${python_bin}" -m instanseg.scripts.train
  --data_path "${dataset_root}"
  --dataset "${dataset_name}"
  --source_dataset "${source_dataset}"
  --output_path "${model_root}"
  --experiment_str "${EXPERIMENT_NAME}"
  --requested_pixel_size "${requested_pixel_size}"
  --target_segmentation "${target_segmentation}"
  --channel_invariant "${channel_invariant}"
  --multihead "${multihead}"
  --augmentation_type "${augmentation_type}"
  --weight "${weight}"
  --batch_size "${batch_size}"
  --num_workers "${num_workers}"
  --num_epochs "${num_epochs}"
  --length_of_epoch "${length_of_epoch}"
  --lr "${learning_rate}"
  --tile_size "${tile_size}"
  --window_size "${window_size}"
  --hotstart_training "${hotstart_training}"
  --rng_seed "${rng_seed}"
  --on_cluster True
)
if [[ "${channel_drop}" != "null" ]]; then
  command+=(--dataset_channel_drop_probabilities "${channel_drop}")
fi
if [[ "${resume_training}" == "1" ]]; then
  command+=(--resume_checkpoint "${resume_checkpoint}")
fi

printf 'command:'
printf ' %q' "${command[@]}"
printf '\n'

if [[ "${TRAINING_DRY_RUN:-0}" == "1" ]]; then
  echo "TRAINING_DRY_RUN=1; command validated but not executed."
  exit 0
fi

exec "${command[@]}"
