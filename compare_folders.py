import os

def find_common_subfolders(folder1, folder2):
    # Get subfolder names in folder1
    subfolders1 = set(os.listdir(folder1))

    # Get subfolder names in folder2
    subfolders2 = set(os.listdir(folder2))

    # Find common subfolders
    common_subfolders = subfolders1.intersection(subfolders2)

    return common_subfolders

def main():
    folder1 = "chemview_archive_8e"
    folder2 = "chemview_archive_section5"

    # Find common subfolders
    common_subfolders = find_common_subfolders(folder1, folder2)

    # Output the result to the console
    if common_subfolders:
        print("Common subfolders:")
        for subfolder in sorted(common_subfolders):
            print(subfolder)
    else:
        print("No common subfolders found.")

if __name__ == "__main__":
    main()
