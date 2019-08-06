#!/bin/sh
set -ue

dir="/tmp/eventdb-sanity"

cd "`dirname "$0"`/.."

runtest()
{
	runs=$1
	expected=$2

	if [ $runs = "e" ]; then
		stack exec -- eventdb-util "$dir" empty
		runs=0
	fi

	rm -rf "$dir"
	i=0
	set +e
	while [ $i -lt $runs ]; do
		result="`stack exec -- eventdb-util "$dir" single triple 2>&1`"
		code=$?
		if [ $code -ne 0 ]; then
			echo "$result"
			exit $code
		fi
		i=`expr $i + 1`
	done
	inspect="`stack exec -- eventdb-util "$dir" inspect 2>&1`"
	code=$?
	if [ $code -ne 0 ] || ! echo "$inspect" | grep -q "Expected count: $expected"; then
		echo -n "Runs: $runs, expected count: $expected\n\n$inspect\n"
		exit $code
	fi
}

stack build --force-dirty --ghc-options '-DBREAKDB_OMIT_COMMIT'
runtest 0 0
runtest e 0
runtest 1 0
runtest 2 0

stack build --force-dirty
runtest 0 0
runtest e 0
runtest 1 4
runtest 2 8

echo -n "\nSuccess!\n"
