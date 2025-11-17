#!/bin/bash

# Define the folder containing the .json files
JSON_FOLDER="downloadsToDo"

# Check if the folder exists
if [ ! -d "$JSON_FOLDER" ]; then
  echo "Folder $JSON_FOLDER does not exist. Exiting."
  exit 1
fi

# Iterate over all .json files in the folder
for json_file in "$JSON_FOLDER"/*.json; do
  # Skip if no .json files are found
  if [ "$json_file" == "$JSON_FOLDER/*.json" ]; then
    echo "No .json files found in $JSON_FOLDER."
    break
  fi

  # Run getFiles.py with the .json file as input
  echo "Processing $json_file..."
  python getFiles.py "$json_file"

  # Rename the .json file to <originalName>.json.done
  mv "$json_file" "$json_file.done"
  echo "Renamed $json_file to $json_file.done"

done

# Completion message
echo "Processing complete."
