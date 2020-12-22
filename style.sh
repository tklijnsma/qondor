pushd "$(dirname $(realpath "${BASH_SOURCE[0]}"))"
black . && isort . && flake8 .
popd