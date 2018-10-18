#!/bin/sh

echo $(pwd)
nohup python $(pwd)/main.py  >/dev/null 2>/dev/null &