#!/usr/bin/env bash
regex="ivy-(.*)\.xml"
IVY_FILENAME="$(find -maxdepth 3 -name "ivy-*.xml")"
if [[ $IVY_FILENAME =~ $regex ]]
    then
        version="${BASH_REMATCH[1]}"
    else
        exit 1
fi
mkdir "./target/scala-2.11/${version}"
cp ./target/scala-2.11/*.jar "./target/scala-2.11/${version}"
cp ./target/scala-2.11/*.pom "./target/scala-2.11/${version}"