#!/bin/bash

# Define the folder containing the .json files
JSON_FOLDER="downloadsToDo"

# Check if the folder exists
if [ ! -d "$JSON_FOLDER" ]; then
  echo "Folder $JSON_FOLDER does not exist. Exiting."
  exit 1
fi

# enable nullglob so unmatched globs produce an empty array
# the (odd) default behavior is to return the glob pattern itself
shopt -s nullglob
files=( "$JSON_FOLDER"/*.json )
if [ ${#files[@]} -eq 0 ]; then
  echo "No .json files found in $JSON_FOLDER"
  exit 2
fi


# Iterate over all .json files in the folder
# as long as we haven't been signalled to stop
for json_file in "${files[@]}"; do
	if [ ! -f ./getFiles.stop ]; then
	  # Run getFiles.py with the .json file as input
	  echo "Processing $json_file..."
	  python getFiles.py "$json_file"
	  rc=$?
	  if [ $rc -ne 0 ]; then
		echo "getFiles.py failed with exit code $rc"
		exit 3
		break
	fi
fi

  if [ ! -f ./getFiles.stop ]; then
    # Rename the .json file to <originalName>.json.done
    # only if our proces is not being stopped by the user
    # There's no harm in leaving it the .json name since
    # re-processing downloaded files is idempotent and fast
    mv "$json_file" "$json_file.done"
    echo "Renamed $json_file to $json_file.done"
  fi

done

# Completion message
echo "Processing complete."
