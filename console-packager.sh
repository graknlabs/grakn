#!/usr/bin/env bash

# inputs
script="$1"
jar="$2"
logback_xml="$3"
output="$4"

# configurations
base_dir="grakn-core-console"
conf_dir="conf"
services_dir="services"
services_dir_lib="lib"

# prepare directories and files
mkdir -p "$base_dir"
mkdir -p "$base_dir/$conf_dir"
mkdir -p "$base_dir/$services_dir/$services_dir_lib"

cp "$script" "$base_dir"
cp "$logback_xml" "$base_dir/$conf_dir"
cp "$jar" "$base_dir/$services_dir/$services_dir_lib"

zip -r "$output" "$base_dir"

# cleanup
rm -r "$base_dir"