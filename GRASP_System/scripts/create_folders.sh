#!/bin/bash
# create_folders.sh
# Creates the GRASP folder structure under /Users/jamesrose/GRASP
# Run once when setting up a new installation.

BASE="/Users/jamesrose/GRASP"

# echo "Creating GRASP folder structure under $BASE..."

mkdir -p "$BASE/GRASP_System/scripts"

mkdir -p "$BASE/GRASP_User/assets"
mkdir -p "$BASE/GRASP_User/config"
mkdir -p "$BASE/GRASP_User/csv"
mkdir -p "$BASE/GRASP_User/gedcoms"
mkdir -p "$BASE/GRASP_User/website"
mkdir -p "$BASE/GRASP_User/examples"

echo "Done. Folder structure created:"
find "$BASE" -type d | sort
