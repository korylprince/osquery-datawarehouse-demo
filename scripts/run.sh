#!/bin/bash

# get directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# terraform repo - dir/../terraform
TF_DIR="$DIR/../terraform"

pushd $TF_DIR > /dev/null
HOSTNAME=$(tofu output -raw cloudflare_root_record)
popd > /dev/null

ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR ubuntu@$HOSTNAME "$@"
