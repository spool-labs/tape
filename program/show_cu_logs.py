import json
from tabulate import tabulate
from pathlib import Path

def fmt(num):
    return f"{num:,}"

def fmt_diff(num):
    sign = "+" if num > 0 else ""
    return f"{sign}{num:,}"

# Path to JSON
base_dir = Path(__file__).parent
json_path = base_dir / "cu_logs.json"

try:
    with open("cu_logs.json") as f:
        data = json.load(f)
except FileNotFoundError:
    print("File not found. No logs to display.")
    data = []

data = data[:3]

for d in data:
    entries = d["entries"]

    # Convert each row: (Instruction, Value, Diff)
    rows = [
        (
            k,
            fmt(v["value"]),
            fmt_diff(v["diff"]),
        )
        for k, v in sorted(entries.items())
    ]

    print()
    print("## Run at", d["timestamp"])
    print()
    print(tabulate(rows, headers=["Instruction", "Compute Units", "Diff"], tablefmt="github"))
    print()
