#!/bin/sh
set -ue

dir=/tmp/eventdb-profile

for d in hp2pretty profiteur; do
	if ! which $d > /dev/null; then
		echo "Missing dependency, install $d" 1>&2
		exit 1
	fi
done

# https://making.pusher.com/top-tips-and-tools-for-optimising-haskell/
# https://making.pusher.com/memory-profiling-in-haskell/

cd "`dirname "$0"`/.."
stack build --profile
rm -rf "$dir" *.prof *.eventlog *.prof.html *.hp *.svg
stack exec -- eventdb-util +RTS -p -N2 -lf -S -s -hd -RTS "$dir" thrash inspect
hp2pretty eventdb-util.hp
profiteur eventdb-util.prof
