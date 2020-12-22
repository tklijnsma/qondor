pushd "$(dirname $(realpath "${BASH_SOURCE[0]}"))"
black . --exclude examples/* && isort . && flake8 .
popd