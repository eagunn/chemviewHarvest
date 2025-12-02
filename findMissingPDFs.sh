find . -type f -name "*.html" -print0 |
while IFS= read -r -d '' html; do
    dir=$(dirname "$html")
    if ! find "$dir" -maxdepth 1 -type f -name "*.pdf" | grep -q .; then
        echo "$dir"
    fi
done | sort -u
