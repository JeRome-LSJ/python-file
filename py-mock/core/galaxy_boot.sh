#!/usr/bin/env bash
# galaxy_boot.sh

echo "shell args: [$*]"

SH_DIR="$(
  cd "$(dirname "$0")"
  pwd
)"
cd "${SH_DIR}"

mkdir -p /tmp/galaxy_boot

: "${PY_VERSION:="python"}"
export PYTHONPATH=pkgs/:/tmp/galaxy_boot:${PYTHONPATH}:.

echo "-------- env --------"
id
env
df -hT
echo '/export' && ls -la /export
echo '/soft/home/' && ls -la /soft/home/
echo '/soft/pkgs/' && ls -la /soft/pkgs/
echo "-------- env --------"

set -x
# 添加自定义包
for pk in pkgs/*.*; do
  echo "add $pk"
  #pk=$(realpath "$pk")
  [[ -f "$pk" ]] && PYTHONPATH=$pk:$PYTHONPATH
done
unset pk

export PYTHONPATH
echo "PYTHONPATH $PYTHONPATH"

echo "-------- pip --------"
PY_PIP="${PY_VERSION} -m pip"
${PY_VERSION} -V
${PY_PIP} -V
${PY_PIP} list

PIP_ARGS="--no-python-version-warning --disable-pip-version-check"
if [ -d "/soft/pkgs" ]; then
  PIP_ARGS="--trusted-host=repos.jd.com --index-url=http://repos.jd.com/pypi/simple"
fi

#${PY_PIP} install -r requirements-amc5k.txt --target=/tmp/galaxy_boot ${PIP_ARGS}

## "futures>=3.1.1; python_version < '3.5'"
## "numpy==1.16.6 pandas==0.24.2"
pkgs=(
  "wheel setuptools==44.1.1"
  "psutil futures"
  "numpy==1.16.6 pandas==0.20.3"
)

for pkg in "${pkgs[@]}"; do
  echo "Install ${pkg}"
  ## --user --target=./pkgs  --no-index
  ${PY_PIP} install --find-links=./pypi/ --target=/tmp/galaxy_boot ${PIP_ARGS}  ${pkg}
done


echo "-------- pip --------"
