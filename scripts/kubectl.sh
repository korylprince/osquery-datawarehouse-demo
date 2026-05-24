#!/bin/bash

# get directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

$DIR/run.sh sudo k3s kubectl $@
