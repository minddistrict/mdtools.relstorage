#!/usr/bin/env bash

set -x
set -e

if test "$#" != "1"; then
    echo "Usage: $0 database"
    exit 1
fi

database="$1"

if test ! -d "done"; then
    mkdir "done";
fi

for file in $(ls todo); do
    psql -q -f "todo/$file" -d "$database"
    if test "$?" != "0"; then
        exit 1
    fi
    mv "todo/$file" "done/$file"
    sleep 5
done
