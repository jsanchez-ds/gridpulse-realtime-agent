#!/usr/bin/env bash
# source before running anything:  source scripts/env.sh

export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-11.0.30.7-hotspot"
export HADOOP_HOME="../Nuevo proyecto/vendor/hadoop"      # reuse Project 1's winutils
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PATH"
export PATH="/c/Program Files/GitHub CLI:$PATH"
export PATH="$(pwd)/.venv/Scripts:$PATH"

export PYSPARK_PYTHON="$(pwd)/.venv/Scripts/python.exe"
export PYSPARK_DRIVER_PYTHON="$(pwd)/.venv/Scripts/python.exe"

echo "[env] JAVA_HOME=$JAVA_HOME"
echo "[env] HADOOP_HOME=$HADOOP_HOME"
echo "[env] python: $(python --version 2>/dev/null)"
echo "[env] gh:     $(gh --version 2>/dev/null | head -1)"
