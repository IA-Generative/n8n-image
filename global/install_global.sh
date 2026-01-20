#!/bin/bash

set -euo pipefail

# parse the package.json to get the list of dependencies and install them globally
# respect version numbers
# install all depencies in one command 
npm install -g $(jq -r '.dependencies | to_entries | map("\(.key)@\(.value)") | join(" ")' package.json)