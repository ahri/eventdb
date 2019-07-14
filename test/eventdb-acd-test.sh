#!/bin/bash
set -ue

dir="/tmp/eventdb-acd-test"

if ! which fiu-run > /dev/null; then
	echo "Missing dependency, install fiu-run, e.g. apt install fiu-utils" 1>&2
	exit 1
fi

if [ $# -ne 1 ] || ! echo $1 | grep -vq '^[0-9]+$'; then
	echo "Pass in max rounds"
	exit 1
fi

rounds=$1

cd "`dirname "$0"`/.."
stack build

for i in `seq 1 $rounds`; do
	echo "RUN $i:"
	rm -rf "$dir"
	stack exec -- \
		fiu-run -x -c 'enable_random name=posix/io/rw/write*,probability=0.005' \
			eventdb-util "$dir" spam \
		|| true

	stack exec -- eventdb-util "$dir" inspect
	let 'i++'
done
