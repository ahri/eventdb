#!/bin/bash
set -ue

dir="/tmp/eventdb-empty-test"

cd "`dirname "$0"`/.."
stack build

rm -rf "$dir"
stack exec -- eventdb-util "$dir" inspect
