#!/bin/sh
set -ue

dir="/tmp/eventdb-sanity"

cd "`dirname "$0"`/.."

runtest()
{
	runs=$1
	expected=$2

	rm -rf "$dir"
	i=0
	while [ $i -lt $runs ]; do
		stack exec -- eventdb-util "$dir" single
		i=`expr $i + 1`
	done
	inspect=`stack exec -- eventdb-util "$dir" inspect`
	if ! echo "$inspect" | grep -q "Expected count: $expected"; then
		echo -n "Runs: $runs, expected: $expected\n\n$inspect"
		exit 1
	fi
}

stack build --force-dirty --ghc-options '-DBREAKDB_OMIT_COMMIT'
runtest 0 0
runtest 1 0
runtest 2 0

stack build --force-dirty
runtest 0 0
runtest 1 1
runtest 2 2

echo -n "\nSuccess!"
