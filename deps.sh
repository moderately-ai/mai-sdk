#!/usr/bin/env bash

set -e

# exit if rustup is not installed
if ! command -v rustup &> /dev/null
then
    echo "❌ rustup could not be found, please install from here: https://rustup.rs/"
    exit
fi

# exit if pyenv is not installed
if ! command -v pyenv &> /dev/null
then
    echo "❌ pyenv could not be found, please install from here: https://github.com/pyenv/pyenv?tab=readme-ov-file#installation"
    exit
fi

# install rust dependencies
echo "👷 installing rust dependencies"
cargo build
echo "✅ rust dependencies installed"

# install python dependencies
echo "👷 installing python dependencies"
pyenv install -s
pyenv exec python -m venv env
env/bin/pip install -q --upgrade pip pip-tools
env/bin/pip-compile -q requirements.in
env/bin/pip install -q -r requirements.txt
echo "✅ python dependencies installed"