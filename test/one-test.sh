#!/bin/bash
set -ue

dir="/tmp/eventdb-one-test"

cd "`dirname "$0"`/.."
stack build

rm -rf "$dir"
stack exec -- eventdb-util "$dir" single inspect
