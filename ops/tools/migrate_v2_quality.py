import json
import os
import glob

QUALITY_DIR = "/opt/quantlab/quality/date=20260103"
files = glob.glob(f"{QUALITY_DIR}/window=*.json")

stats = {"total": 0, "changed": 0, "v1_counts": {"GOOD": 0, "DEGRADED": 0, "BAD": 0}, "v2_counts": {"GOOD": 0, "DEGRADED": 0, "BAD": 0}}

for file_path in files:
    with open(file_path, "r") as f:
        data = json.load(f)
    
    old_quality = data["quality"]
    stats["v1_counts"][old_quality] += 1
    
    sig = data["signals"]
    max_offline = max(sig.get("offline_seconds_by_exchange", {"none": 0}).values())
    peak_q = sig.get("queue_pct_peak", 0)
    dropped = sig.get("dropped_events", 0)
    reconnects = sig.get("reconnects", 0)
    acc_drain = sig.get("drain_mode_accelerated_seconds", 0)
    
    # v2 Logic
    new_quality = "GOOD"
    if dropped > 0 or max_offline > 120 or peak_q >= 80:
        new_quality = "BAD"
    elif reconnects >= 3 or max_offline > 60 or acc_drain > 60:
        new_quality = "DEGRADED"
    
    stats["v2_counts"][new_quality] += 1
    stats["total"] += 1
    
    if old_quality != new_quality:
        data["quality"] = new_quality
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        stats["changed"] += 1
        print(f"Updated {os.path.basename(file_path)}: {old_quality} -> {new_quality}")

print("\nMigration Summary:")
print(f"Total files processed: {stats['total']}")
print(f"Total files updated:   {stats['changed']}")
print(f"V1 quality counts:     {stats['v1_counts']}")
print(f"V2 quality counts:     {stats['v2_counts']}")
