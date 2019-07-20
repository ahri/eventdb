#!/bin/bash
set -ue

dir="/tmp/eventdb-two-test"

cd "`dirname "$0"`/.."
stack build

rm -rf "$dir"
stack exec -- eventdb-util "$dir" single single inspect
