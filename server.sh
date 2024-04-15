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

host="0.0.0.0"
port=8000
cmd="python -m uvicorn main:app --host $host --port $port --loop asyncio"

if [ -z "$config_path" ]; then
    $cmd
else
    CONFIG_PATH=$config_path $cmd
fi
