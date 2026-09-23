# ⚡ Switchboard AI (MVP 1)

> **FinOps Smart Compute Router & Proxy for Databricks Lakehouses**

Switchboard AI intercepts incoming SQL workloads, inspects dataset sizes and AST complexity in real time, and routes sub-10GB jobs to high-performance single-node engines (**DuckDB**) while delegating heavy workloads to **Databricks PySpark**.

---

## 🎯 Benchmark Results (Sub-10GB Query)

| Engine | Execution Latency | Spin-up Delay | Estimated Cost |
| :--- | :--- | :--- | :--- |
| **DuckDB (Switchboard)** | **~0.18s** | **0 ms** | **~$0.00** |
| **PySpark (Local JVM)** | ~14.0s | ~12–15s | High (Cluster DBUs) |

---

## 🚀 Key Features Built in MVP 1

* **AST Parsing (`SQLGlot`):** Extracts referenced table names (handling aliases) and inspects syntax complexity.
* **Metadata Estimator (`delta-rs`):** Inspects underlying Delta/Parquet storage metrics without scanning data into memory.
* **Conditional Routing Engine:** Auto-routes workloads based on configurable single-node RAM thresholds.
* **Execution & Benchmarking:** Registers temporary Parquet views and executes queries on DuckDB with microsecond-level overhead.

---

## 🛠️ Local Setup & Execution

```bash
# Clone the repository
git clone [https://github.com/YOUR_USERNAME/switchboard-ai.git](https://github.com/YOUR_USERNAME/switchboard-ai.git)
cd switchboard-ai

# Set up environment
python -m venv venv
source venv/bin/activate  # On Windows: .\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run MVP 1 Router & Benchmark
python switchboard_core.py
