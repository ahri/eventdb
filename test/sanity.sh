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
	if [ $code -ne 0 ] || ! echo "$inspect" | grep -q "Index count: $expected"; then
		echo -n "Runs: $runs, expected count: $expected\n\n$inspect\n"
		exit $code
	fi
	set -e
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

mem_profile_dir=/tmp/eventdb-mem-profile
for app in \
		bank-acct-demo \
		client-demo \
		"mem-profile-file-write $mem_profile_dir 2" \
		"mem-profile-file-read $mem_profile_dir" \
		"mem-profile-stream-read $mem_profile_dir 2" \
		; do
	stack exec -- $app
done

 haddock_cover=`stack haddock 2>&1 | grep "[0-9]*% ( [0-9]* / [0-9]*) in 'Database\\.EventDB'"`
 if ! echo $haddock_cover | grep -q 100%; then
	 echo "Incomplete haddocks:"
	 echo $haddock_cover
	 exit 1
 fi

echo -n "\nSuccess!\n"
