import json

input_path = "data/part2_1gb.txt"
output_path = "data/part2_1gb_fixed.txt"

fixed = 0
buffer = ""

with open(input_path, "r", encoding="utf-8") as infile, \
     open(output_path, "w", encoding="utf-8") as outfile:
    for line in infile:
        buffer += line.strip()
        try:
            obj = json.loads(buffer)
            outfile.write(json.dumps(obj) + "\n")
            buffer = ""
            fixed += 1
        except json.JSONDecodeError:
            continue

print(f"✅ Fixed {fixed} records → {output_path}")

