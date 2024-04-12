#!/bin/bash

config_path=""

helpFunction()
{
    echo "Usage $0 -c \"config_path\""
    exit 1
}

while getopts "c:" opt
do
    case "$opt" in
        c) config_path=$OPTARG ;;
        *) echo "Invalid option" ; helpFunction ;;
    esac
done

if [ -z "$config_path" ]; then
    python -m uvicorn main:app --host 0.0.0.0 --port 8000
else
    CONFIG_PATH=$config_path python -m uvicorn main:app --host 0.0.0.0 --port 8000
fi
