#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUNNER="${REPO_ROOT}/scripts/run_instanseg_training_job.sh"
MATRIX_PATH="${REPO_ROOT}/training/instanseg_experiments.json"
FORK_ROOT="${INSTANSEG_FORK_ROOT:-/data1/lowes/ratnayn/Codex/projects/instanseg}"

PARTITION="gpu"
GPUS="1"
GRES=""
CONSTRAINT=""
CPUS="24"
MEMORY="128G"
WALLTIME="7-00:00:00"
SUBMIT=0
RESUME=0
BATCH_NAME="$(date '+%Y%m%d_%H%M%S')"
declare -a REQUESTED_EXPERIMENTS=()
EXPLICIT_EXPERIMENTS=0

usage() {
  cat <<'EOF'
Usage: submit_instanseg_training_matrix.sh [options]

By default this prints a plan for every ready experiment and submits nothing.

Options:
  --experiment NAME    Select one experiment; repeatable.
  --matrix PATH        Experiment JSON file.
  --fork-root PATH     InstanSeg source to snapshot (default: local fork).
  --partition NAME     SLURM partition (default: gpu).
  --gpus N             Generic GPU count (default: 1).
  --gres VALUE         Optional typed GRES request; replaces --gpus when set.
  --constraint VALUE   Optional SLURM node-feature constraint (for example h100|h200).
  --cpus N             CPUs per task (default: 24).
  --mem VALUE          Memory request (default: 128G).
  --time VALUE         Wall time (default: 7-00:00:00).
  --batch-name NAME    Name for logs and the immutable source snapshot.
  --resume             Resume one explicitly selected interrupted experiment.
  --submit             Create a source snapshot and submit jobs with sbatch.
  -h, --help           Show this help.

Blocked experiments are never selected automatically. To run one after resolving
its blocker, mark it ready in the matrix so the decision is preserved in git.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --experiment)
      REQUESTED_EXPERIMENTS+=("$2")
      EXPLICIT_EXPERIMENTS=$((EXPLICIT_EXPERIMENTS + 1))
      shift 2
      ;;
    --matrix)
      MATRIX_PATH="$2"
      shift 2
      ;;
    --partition)
      PARTITION="$2"
      shift 2
      ;;
    --fork-root)
      FORK_ROOT="$2"
      shift 2
      ;;
    --gres)
      GRES="$2"
      shift 2
      ;;
    --gpus)
      GPUS="$2"
      shift 2
      ;;
    --constraint)
      CONSTRAINT="$2"
      shift 2
      ;;
    --cpus)
      CPUS="$2"
      shift 2
      ;;
    --mem)
      MEMORY="$2"
      shift 2
      ;;
    --time)
      WALLTIME="$2"
      shift 2
      ;;
    --batch-name)
      BATCH_NAME="$2"
      shift 2
      ;;
    --resume)
      RESUME=1
      shift
      ;;
    --submit)
      SUBMIT=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! -f "${MATRIX_PATH}" ]]; then
  echo "Missing matrix: ${MATRIX_PATH}" >&2
  exit 2
fi
if [[ ! -x "${RUNNER}" ]]; then
  echo "Runner is not executable: ${RUNNER}" >&2
  exit 2
fi
if [[ ! -f "${FORK_ROOT}/instanseg/__init__.py" ]]; then
  echo "InstanSeg fork root is invalid: ${FORK_ROOT}" >&2
  exit 2
fi
if [[ ! "${GPUS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "--gpus must be a positive integer: ${GPUS}" >&2
  exit 2
fi

if [[ ${#REQUESTED_EXPERIMENTS[@]} -eq 0 ]]; then
  mapfile -t REQUESTED_EXPERIMENTS < <(jq -r '.experiments[] | select(.ready == true) | .name' "${MATRIX_PATH}")
fi
if [[ "${RESUME}" -eq 1 && ("${EXPLICIT_EXPERIMENTS}" -ne 1 || "${#REQUESTED_EXPERIMENTS[@]}" -ne 1) ]]; then
  echo "--resume requires exactly one explicit --experiment NAME." >&2
  exit 2
fi
if [[ ${#REQUESTED_EXPERIMENTS[@]} -eq 0 ]]; then
  echo "No experiments selected." >&2
  exit 2
fi

training_root="$(jq -er '.defaults.training_root' "${MATRIX_PATH}")"
batch_root="${training_root}/slurm_runs/${BATCH_NAME}"
log_root="${batch_root}/logs"
snapshot_root="${batch_root}/source/instanseg"

if [[ "${SUBMIT}" -eq 1 ]]; then
  if [[ -e "${batch_root}" ]]; then
    echo "Batch directory already exists: ${batch_root}" >&2
    exit 2
  fi
  mkdir -p "${log_root}" "$(dirname "${snapshot_root}")"
  rsync -a --exclude='.git/' --exclude='__pycache__/' --exclude='.pytest_cache/' \
    "${FORK_ROOT}/" "${snapshot_root}/"
  if git -C "${FORK_ROOT}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git -C "${FORK_ROOT}" rev-parse HEAD > "${batch_root}/fork_commit.txt"
    git -C "${FORK_ROOT}" status --short > "${batch_root}/fork_status.txt"
    git -C "${FORK_ROOT}" diff --binary > "${batch_root}/fork_worktree.patch"
  else
    prior_batch_root="$(dirname "$(dirname "${FORK_ROOT}")")"
    if [[ -f "${prior_batch_root}/fork_commit.txt" ]]; then
      cp "${prior_batch_root}/fork_commit.txt" "${batch_root}/fork_commit.txt"
      cp "${prior_batch_root}/fork_status.txt" "${batch_root}/fork_status.txt"
      cp "${prior_batch_root}/fork_worktree.patch" "${batch_root}/fork_worktree.patch"
    else
      printf 'non-git source snapshot: %s\n' "${FORK_ROOT}" > "${batch_root}/fork_commit.txt"
      : > "${batch_root}/fork_status.txt"
      : > "${batch_root}/fork_worktree.patch"
    fi
  fi
  printf '%s\n' "${FORK_ROOT}" > "${batch_root}/source_origin.txt"
  cp "${MATRIX_PATH}" "${batch_root}/experiment_matrix.json"
  cp "${RUNNER}" "${batch_root}/run_instanseg_training_job.sh"
  chmod +x "${batch_root}/run_instanseg_training_job.sh"
  runner_for_jobs="${batch_root}/run_instanseg_training_job.sh"
  matrix_for_jobs="${batch_root}/experiment_matrix.json"
else
  log_root="${batch_root}/logs"
  snapshot_root="${FORK_ROOT}"
  runner_for_jobs="${RUNNER}"
  matrix_for_jobs="${MATRIX_PATH}"
fi

printf 'Mode: %s\n' "$([[ "${SUBMIT}" -eq 1 ]] && printf submit || printf plan-only)"
printf 'Batch: %s\n' "${batch_root}"
printf 'Source: %s\n' "${snapshot_root}"
printf 'Resume: %s\n' "$([[ "${RESUME}" -eq 1 ]] && printf yes || printf no)"
if [[ -n "${GRES}" ]]; then
  gpu_request="gres=${GRES}"
else
  gpu_request="gpus=${GPUS} (generic)"
fi
if [[ -n "${CONSTRAINT}" ]]; then
  constraint_request=" constraint=${CONSTRAINT}"
else
  constraint_request=""
fi
printf 'Resources: partition=%s %s%s cpus=%s mem=%s time=%s\n' \
  "${PARTITION}" "${gpu_request}" "${constraint_request}" "${CPUS}" "${MEMORY}" "${WALLTIME}"

for experiment in "${REQUESTED_EXPERIMENTS[@]}"; do
  experiment_json="$(jq -ce --arg name "${experiment}" '.experiments[] | select(.name == $name)' "${MATRIX_PATH}")" || {
    echo "Unknown experiment: ${experiment}" >&2
    exit 2
  }
  if [[ "$(jq -r '.ready' <<< "${experiment_json}")" != "true" ]]; then
    echo "Refusing blocked experiment '${experiment}': $(jq -r '.blocked_reason' <<< "${experiment_json}")" >&2
    exit 2
  fi

  stdout_path="${log_root}/${experiment}.%j.out"
  stderr_path="${log_root}/${experiment}.%j.err"
  if [[ "${RESUME}" -eq 1 ]]; then
    resume_prefix="RESUME_TRAINING=1 "
  else
    resume_prefix=""
  fi
  wrap_command="${resume_prefix}INSTANSEG_SOURCE_ROOT=$(printf '%q' "${snapshot_root}") $(printf '%q' "${runner_for_jobs}") $(printf '%q' "${experiment}") $(printf '%q' "${matrix_for_jobs}")"
  sbatch_command=(
    sbatch
    --job-name "is_${experiment}"
    --partition "${PARTITION}"
    --cpus-per-task "${CPUS}"
    --mem "${MEMORY}"
    --time "${WALLTIME}"
    --output "${stdout_path}"
    --error "${stderr_path}"
    --wrap "${wrap_command}"
  )
  if [[ -n "${GRES}" ]]; then
    sbatch_command+=(--gres "${GRES}")
  else
    sbatch_command+=(--gpus "${GPUS}")
  fi
  if [[ -n "${CONSTRAINT}" ]]; then
    sbatch_command+=(--constraint "${CONSTRAINT}")
  fi

  printf ' '
  printf '%q ' "${sbatch_command[@]}"
  printf '\n'
  if [[ "${SUBMIT}" -eq 1 ]]; then
    "${sbatch_command[@]}"
  fi
done
