import os

input_file = "data/part3_1_5gb.txt"  # <- Full path to your input file

output_files = [
    ("data/part1_500mb.txt", 500 * 1024 * 1024),
    ("data/part2_1gb.txt", 1 * 1024 * 1024 * 1024),
]

with open(input_file, "r", encoding="utf-8") as infile:
    for filename, max_bytes in output_files:
        with open(filename, "w", encoding="utf-8") as out:
            written = 0
            while written < max_bytes:
                line = infile.readline()
                if not line:
                    break
                encoded_line = line.encode("utf-8")
                written += len(encoded_line)
                out.write(line)

print("✅ Split complete: 500MB, 1GB files created in data/")
