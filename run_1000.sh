#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://127.0.0.1:8081}"
#JOBS=2000
#SEED_BASE=45
#RUNS=0
#BATCH_SIZE=40
#BATCH_INTERVAL=20

#ALGOS=("ECOTASK" "PSO_ACO_GA" "PSO" "ACO" "GA" "GREEDY")
#
#mkdir -p results/case2_fast
#echo "=== CASE 2 FAST: 2000 tasks + SAME INPUT + KIỂM TRA FILE TỒN TẠI ==="
#
#for run in $(seq 1 $RUNS); do
#  seed_run=$((SEED_BASE + run))
#  echo "================================================================"
#  echo "RUN $run / $RUNS | SEED = $seed_run"
#  echo "================================================================"
#
#  TASKS_JSON="results/case2_fast/tasks_run${run}_seed${seed_run}.json"
#  # ==================== 1. GENERATE TASKS (chỉ khi chưa tồn tại) ====================
#  if [ ! -f "$TASKS_JSON" ]; then
#    echo "Generating new tasks for this run (seed = $seed_run)..."
#    python3 - <<PY
#import json, random
#rng = random.Random($seed_run)
#tasks = []
#popular_types = [1, 2, 3]
#for i in range($JOBS):
#    r = rng.random()
#    if r < 0.4:
#        task_type = rng.choice(popular_types)
#    else:
#        task_type = rng.randint(1, 7)
#    base_duration = 3
#    tasks.append({
#        "type": task_type,
#        "duration": base_duration,
#        "input_mode": "camera"
#    })
#with open("$TASKS_JSON", "w") as f:
#    json.dump(tasks, f)
#print(f"→ Saved {len(tasks)} tasks for RUN $run")
#PY
#  else
#    echo "→ Using existing tasks file: $TASKS_JSON (skip generate)"
#  fi
#
#  # ==================== 2. IN RA TỔNG SỐ LƯỢNG TỪNG TASKTYPE ====================
#  echo "Task type distribution for this run:"
#  python3 - <<COUNT
#import json
#from collections import Counter
#with open("$TASKS_JSON") as f:
#    tasks = json.load(f)
#type_count = Counter(t["type"] for t in tasks)
#total = len(tasks)
#print("Type | Count | Percentage")
#print("-" * 35)
#for t in sorted(type_count.keys()):
#    pct = type_count[t] / total * 100
#    print(f"{t:4} | {type_count[t]:5} | {pct:6.2f}%")
#print(f"Total tasks: {total}\n")
#COUNT
#
#  # ==================== 3. CHẠY TẤT CẢ ALGOS ====================
#  for algo in "${ALGOS[@]}"; do
#    echo "→ Running $algo (seed $seed_run)"
#    docker compose down -v --remove-orphans --timeout 30 2>/dev/null || true
#    docker rm -f $(docker ps -a -q --filter "name=worker-") 2>/dev/null || true
#
#    SCHEDULER_MODE=$algo docker compose up -d --build
#    until curl -s "$API_BASE/api/jobs/available" >/dev/null; do sleep 2; done
#    sleep 5
#
#    # Submit tasks
#    python3 - <<PY2
#import json, time, urllib.request
#api = "$API_BASE"
#with open("$TASKS_JSON") as f:
#    tasks = json.load(f)
#submitted = 0
#while submitted < len(tasks):
#    batch_size = min($BATCH_SIZE, len(tasks) - submitted)
#    for _ in range(batch_size):
#        if submitted >= len(tasks): break
#        task = tasks[submitted]
#        urllib.request.urlopen(urllib.request.Request(
#            api + "/api/jobs",
#            data=json.dumps(task).encode(),
#            headers={"Content-Type": "application/json"},
#            method="POST"
#        ))
#        submitted += 1
#        if submitted % 200 == 0:
#            print(f"Submitted {submitted}/{len(tasks)}")
#    if submitted < len(tasks):
#        time.sleep($BATCH_INTERVAL)
#PY2
#
#    # Chờ hoàn thành
#    timeout=2500
#    start=$(date +%s)
#    while true; do
#      rep=$(curl -s "$API_BASE/api/eval/report")
#      done=$(echo $rep | jq '.total_completed + .total_failed + .total_stopped' 2>/dev/null || echo 0)
#      if [ "$done" -ge "$JOBS" ]; then break; fi
#      if [ $(( $(date +%s) - start )) -gt $timeout ]; then echo "Timeout!"; break; fi
#      sleep 3
#    done
#
#    JSON_FILE="results/case2_fast/report_${algo}_run${run}_seed${seed_run}.json"
#    curl -s "$API_BASE/api/eval/report" | jq '.' > "$JSON_FILE"
#    echo "→ Saved: $JSON_FILE"
#
#    # ==================== 4. TÍNH VÀ IN TỔNG BATCHCOUNT ====================
#    total_batch=$(jq '.batching_count_per_worker | add // 0' "$JSON_FILE" 2>/dev/null || echo 0)
#    echo "→ Total batching count for $algo: $total_batch"
#
#    sleep 5
#  done
#done
#
#echo "=================================================="
#echo "HOÀN TẤT! Mỗi RUN dùng file tasks riêng (nếu đã tồn tại thì reuse)."
#echo "Kết quả nằm trong results/case2_fast/"
#echo "================================"
#echo "Analyzing results"
#echo "================================"
#
#python3 - <<PY
#import json, glob, os
#import pandas as pd
#import numpy as np
#import matplotlib.pyplot as plt
#
#files = glob.glob("results/case2_fast/report_*_run*_seed*.json")
#
#rows = []
#
#for f in files:
#    name = os.path.basename(f).replace("report_", "").replace(".json", "")
#
#    parts = name.split("_run")
#    algo = parts[0]
#
#    run_part = parts[1]
#    run = int(run_part.split("_seed")[0])
#    seed = int(run_part.split("_seed")[1])
#
#    with open(f) as fp:
#        r = json.load(fp)
#
#    rows.append({
#        "algorithm": algo,
#        "run": run,
#        "seed": seed,
#        "throughput": r.get("throughput_jobs_per_sec", 0),
#        "makespan": r.get("makespan_sec", 0),
#        "latency_mean": r.get("latency_mean_sec", 0),
#        "latency_p95": r.get("latency_p95_sec", 0),
#        "energy": r.get("total_energy_joules", 0),
#        "gini_assign": r.get("gini_assignments", 0),
#        "processing_power": r.get("processing_power_gc_per_sec", 0)
#    })
#
#df = pd.DataFrame(rows)
#
#metrics = [
#    "throughput",
#    "makespan",
#    "latency_mean",
#    "latency_p95",
#    "energy",
#    "gini_assign",
#    "processing_power"
#]
#
#os.makedirs("results/per_seed", exist_ok=True)
#
#print("\n======================================")
#print("BẢNG THEO TỪNG SEED")
#print("======================================")
#
## =========================
## TABLE + CHART PER SEED
## =========================
#
#for seed in sorted(df["seed"].unique()):
#
#    print(f"\n========== SEED {seed} ==========")
#
#    seed_df = df[df["seed"] == seed].sort_values("algorithm")
#
#    table = seed_df[["algorithm"] + metrics]
#
#    print(table.to_string(index=False))
#
#    csv_path = f"results/per_seed/metrics_seed_{seed}.csv"
#    table.to_csv(csv_path, index=False)
#
#    print(f"Saved table → {csv_path}")
#
#    # =========================
#    # PLOT
#    # =========================
#
#    for m in metrics:
#
#        plt.figure(figsize=(8,5))
#
#        plt.bar(
#            seed_df["algorithm"],
#            seed_df[m]
#        )
#
#        plt.title(f"{m} - Seed {seed}")
#        plt.ylabel(m)
#        plt.xticks(rotation=45)
#
#        plt.tight_layout()
#
#        fig_path = f"results/per_seed/bar_{m}_seed_{seed}.png"
#
#        plt.savefig(fig_path)
#        plt.close()
#
#print("\n======================================")
#print("TỔNG HỢP MEAN ± STD")
#print("======================================")
#
#summary = df.groupby("algorithm")[metrics].agg(["mean", "std"])
#
#summary.to_csv("results/metrics_summary.csv")
#
#print(summary)
#
## =========================
## GLOBAL CHART
## =========================
#
#os.makedirs("results/global", exist_ok=True)
#
#for m in metrics:
#
#    plt.figure(figsize=(8,5))
#
#    means = df.groupby("algorithm")[m].mean()
#
#    plt.bar(means.index, means.values)
#
#    plt.title(f"{m} (mean over seeds)")
#    plt.ylabel(m)
#    plt.xticks(rotation=45)
#
#    plt.tight_layout()
#
#    plt.savefig(f"results/global/bar_{m}_mean.png")
#    plt.close()
#
#print("\nCharts saved:")
#print("results/per_seed/")
#print("results/global/")
#print("results/metrics_summary.csv")
#
#PY

# ==================== 5. SO SÁNH 1000 vs 2000 TASKS ====================
echo "=================================================="
echo "SO SÁNH GIỮA 1000 TASKS VÀ 2000 TASKS"
echo "=================================================="

mkdir -p results/comparison

python3 - <<PY_COMPARE
import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np

# Đọc hai file summary (cấu trúc CSV của bạn)
df1000 = pd.read_csv("results/metrics_summary_1000.csv", header=[0,1], index_col=0)
df2000 = pd.read_csv("results/metrics_summary_2000.csv", header=[0,1], index_col=0)

# Chỉ lấy cột "mean" (bỏ std)
mean_cols = [col for col in df1000.columns if col[1] == "mean"]
df1000_mean = df1000[mean_cols].droplevel(1, axis=1)
df2000_mean = df2000[mean_cols].droplevel(1, axis=1)

# Tên metrics sạch
metrics = ["throughput", "makespan", "latency_mean", "latency_p95",
           "energy", "gini_assign", "processing_power"]

# Bảng so sánh
comparison = pd.concat([
    df1000_mean[metrics].add_suffix("_1000"),
    df2000_mean[metrics].add_suffix("_2000")
], axis=1)

print("\n=== BẢNG SO SÁNH MEAN (1000 tasks vs 2000 tasks) ===")
print(comparison.round(4))
comparison.to_csv("results/comparison/comparison_1000_vs_2000.csv")

# ==================== VẼ BIỂU ĐỒ SO SÁNH ====================
os.makedirs("results/comparison", exist_ok=True)
algorithms = df1000_mean.index.tolist()

for metric in metrics:
    plt.figure(figsize=(11, 6))

    x = np.arange(len(algorithms))
    width = 0.35

    plt.bar(x - width/2, df1000_mean[metric], width,
            label='1000 tasks', color='#1f77b4', alpha=0.95)
    plt.bar(x + width/2, df2000_mean[metric], width,
            label='2000 tasks', color='#ff7f0e', alpha=0.95)

    plt.xlabel('Algorithm', fontsize=12)
    plt.ylabel(metric.replace('_', ' ').title(), fontsize=12)
    plt.title(f'{metric.replace("_", " ").title()} — Comparison 1000 vs 2000 tasks', fontsize=14)
    plt.xticks(x, algorithms, rotation=45, ha='right')
    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()

    save_path = f"results/comparison/{metric}_1000_vs_2000.png"
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: {save_path}")

print("\n✅ Hoàn tất so sánh 1000 vs 2000 tasks!")
print("   → Bảng so sánh: results/comparison/comparison_1000_vs_2000.csv")
print("   → Tất cả biểu đồ: results/comparison/")
PY_COMPARE