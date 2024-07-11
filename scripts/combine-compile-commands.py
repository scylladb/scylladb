import argparse
import json
import os

ap = argparse.ArgumentParser(description='Combine multiple compile_commands.json')
ap.add_argument('--buildtype', dest='buildtype', default='dev', help='Type of build directory {dev, release, debug}')

args = ap.parse_args()

builddir = os.path.abspath(f"./build/{args.buildtype}")

if not os.path.exists(builddir):
    raise Exception(f"{builddir} not found")

# List of directories containing compile_commands.json files
directories = [
    builddir,
    os.path.join(builddir, "seastar"),
    os.path.join(builddir, "abseil")
]

combined_commands = []

for directory in directories:
    print(directory)
    file_path = f"{directory}/compile_commands.json"
    try:
        with open(file_path, 'r') as f:
            commands = json.load(f)
            combined_commands.extend(commands)
    except FileNotFoundError:
        print(f"File {file_path} not found. Skipping...")

output_path = "./final_compile_commands.json"
with open(output_path, 'w') as f:
    json.dump(combined_commands, f, indent=2)

print(f"Combined compile_commands.json written successfully to {output_path}.")
