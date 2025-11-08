"""toggle_flag.py

Toggle the presence of a "harvest.go" / "harvest.stop" sentinel file in the current working directory.

Behavior:
- If "harvest.go" exists, rename it to "harvest.stop" and print the action.
- Else if "harvest.stop" exists, rename it to "harvest.go" and print the action.
- Else, create an empty "harvest.go" and print that it was created.

Exit codes:
- 0 success
- 1 unexpected error
"""
from pathlib import Path
import sys

def main():
    cwd = Path.cwd()
    go = cwd / 'harvest.go'
    stop = cwd / 'harvest.stop'

    try:
        if go.exists():
            # rename go -> stop
            go.replace(stop)
            print(f"Renamed: {go.name} -> {stop.name}")
            return 0
        elif stop.exists():
            stop.replace(go)
            print(f"Renamed: {stop.name} -> {go.name}")
            return 0
        else:
            # create empty harvest.go
            go.touch()
            print(f"Created: {go.name}")
            return 0
    except Exception as e:
        print(f"Error while toggling flag: {e}", file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(main())

