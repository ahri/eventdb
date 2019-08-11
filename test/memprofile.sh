#!/bin/sh
set -ue

dir=/tmp/eventdb-mem-profile

for d in hp2pretty xdg-open; do
	if ! which $d > /dev/null; then
		echo "Missing dependency, install $d" 1>&2
		exit 1
	fi
done

# https://making.pusher.com/memory-profiling-in-haskell/

cd "`dirname "$0"`/.."
stack build --profile
rm -rf "$dir" *.prof *.eventlog *.prof.html *.hp *.svg
stack exec -- mem-profile-file-write +RTS -p -N2 -lf -S -s -hd -RTS "$dir" 30
stack exec -- mem-profile-file-read +RTS -p -N2 -lf -S -s -hd -RTS "$dir"
stack exec -- mem-profile-stream-read +RTS -p -N2 -lf -S -s -hd -RTS "$dir" 30
for hp in *.hp; do
	hp2pretty "$hp"
done

chromium-browser *.svg
