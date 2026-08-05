import csv
import random
from datetime import datetime, timedelta

#Settings
NUM_MACHINES = 20
DAYS = 30
READING_INTERVAL_MINUTES = 15
OUTPUT_FILE = "sensor_data.csv"

FAULTY_MACHINES = [3, 11, 17] # these machines will develope a fault over time

start_time = datetime(2026, 1, 1, 0, 0)
total_readings_per_machine = int((DAYS * 24 * 60) / READING_INTERVAL_MINUTES)

rows = []


for machine_id in range(1, NUM_MACHINES + 1):
    current_time = start_time
    is_faulty = machine_id in FAULTY_MACHINES

    for reading_num in range(total_readings_per_machine):
        # Progress from 0.0 (start) to 1.0 (end) across the 30 days
        progress = reading_num / total_readings_per_machine

        if is_faulty:
            # Vibration and temperature climb together as the fault develops
            vibration = round(0.8 + progress * 4.5 + random.uniform(-0.15, 0.15), 3)
            bearing_temp = round(45 + progress * 50 + random.uniform(-2, 2), 2)
            current_draw = round(12 + progress * 4  + random.uniform(-0.5, 0.5), 2)
        else:
            # Healthy machine: stable, minor natural variation
            vibration = round(random.uniform(0.5, 1.4),3)
            bearing_temp = round(random.uniform(40, 70), 2)
            current_draw = round(random.uniform(10, 15), 2)

        rotational_speed = round(1460 + random.uniform(-8, 8), 1)

        # Status mostly Running, occasionally Idle/Maintenance, Fault if late-stage fault
        if is_faulty and progress > 0.85:
            status = "Fault"
        else:
            status = random.choices(["Running", "Idle", "Maintenance"], weights = [85, 10, 5]) [0]

        rows.append([machine_id, current_time, vibration, bearing_temp, current_draw, rotational_speed, status])
        current_time += timedelta(minutes=READING_INTERVAL_MINUTES)

# --- INJECT REALISTIC MESSINESS

# 1. Missing Values (~2% of rows, blank out one random field)
for _ in range(int(len(rows) * 0.02)):
    idx = random.randint(0, len(rows) - 1)
    col = random.randint(2, 5) # don't blank MachineID, Timestamp, or Status
    rows[idx][col] = ""

# 2. Duplicate Rows (~1%)
duplicates = [random.choice(rows).copy() for _ in range(int(len(rows) * 0.01))]
rows.extend(duplicates)

# 3. Sensor Glitches (~0.5%, Impossible Values)
for _ in range(int(len(rows) * 0.005)):
    idx = random.randint(0, len(rows) - 1)
    rows[idx][2] = -999 # impossible vibration reading

random.shuffle(rows) # mix duplicates back in, not just append at the end


# Write to CSV
with open(OUTPUT_FILE, "w", newline = "") as f:
    writer = csv.writer(f)
    writer.writerow(["MachineID", "Timestamp", "VibrationVelocity", "BearingTemperature", "MotorCurrentDraw", "RotationalSpeed", "Status"])
    writer.writerows(rows)

print(f"Generated {len(rows)} rows across {NUM_MACHINES} machines, saved to {OUTPUT_FILE}")
print(f"Faulty machines (developing bearing wear): {FAULTY_MACHINES}")

            

