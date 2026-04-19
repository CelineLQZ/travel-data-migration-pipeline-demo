"""
generate_dashboard.py
---------------------
Generates a self-contained HTML dashboard with pipeline metrics.
Called by validation/final_report.py at the end of the pipeline.

The output file can be opened in any browser — no server required.

Charts:
  1. Bar chart: source vs staging vs error rows per table
  2. Horizontal bar chart: pipeline step durations in seconds
  3. Summary table: SLO status (PASSED / FAILED) per table

Usage:
    from monitoring.generate_dashboard import generate_dashboard
    generate_dashboard(metrics, run_timestamp)
"""

import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def generate_dashboard(metrics: dict, run_timestamp: str) -> str:
    """
    Build and save the HTML dashboard.

    Args:
        metrics: dict with structure:
            {
              "tables": {
                "hotels":    {"source": 50,  "staging": 47, "errors": 3,  "slo": "PASSED"},
                "customers": {"source": 200, "staging": 188,"errors": 12, "slo": "PASSED"},
                "bookings":  {"source": 500, "staging": 468,"errors": 32, "slo": "PASSED"},
              },
              "durations": {
                "generate_data": 2.1,
                "ingest_to_s3":  5.4,
                "transform_hotels":    12.3,
                "transform_customers": 14.1,
                "transform_bookings":  18.7,
                "load_snowflake_raw":   9.2,
                "load_snowflake_staging": 10.5,
                "build_analytics_table":   4.1,
              }
            }
        run_timestamp: string like "2024-06-01_12-30-00"

    Returns:
        Path to the saved HTML file.
    """
    tables = list(metrics["tables"].keys())
    source_counts  = [metrics["tables"][t]["source"]  for t in tables]
    staging_counts = [metrics["tables"][t]["staging"] for t in tables]
    error_counts   = [metrics["tables"][t]["errors"]  for t in tables]
    slo_statuses   = [metrics["tables"][t]["slo"]     for t in tables]

    durations  = metrics.get("durations", {})
    step_names = list(durations.keys())
    step_secs  = list(durations.values())

    # Create a 3-row subplot layout
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=[
            "Row Counts: Source vs Staging vs Errors",
            "Pipeline Step Durations (seconds)",
            "SLO Status per Table",
        ],
        specs=[[{"type": "xy"}], [{"type": "xy"}], [{"type": "table"}]],
        vertical_spacing=0.12,
        row_heights=[0.4, 0.35, 0.25],
    )

    # --- Chart 1: grouped bar chart — row counts per table ---
    fig.add_trace(go.Bar(name="Source",  x=tables, y=source_counts,  marker_color="#4C78A8"), row=1, col=1)
    fig.add_trace(go.Bar(name="Staging", x=tables, y=staging_counts, marker_color="#54A24B"), row=1, col=1)
    fig.add_trace(go.Bar(name="Errors",  x=tables, y=error_counts,   marker_color="#E45756"), row=1, col=1)
    fig.update_layout(barmode="group")

    # --- Chart 2: horizontal bar chart — step durations ---
    fig.add_trace(
        go.Bar(
            name="Duration (s)",
            x=step_secs,
            y=step_names,
            orientation="h",
            marker_color="#72B7B2",
            showlegend=False,
        ),
        row=2, col=1,
    )

    # --- Chart 3: SLO status table ---
    slo_colors = ["#54A24B" if s == "PASSED" else "#E45756" for s in slo_statuses]
    fig.add_trace(
        go.Table(
            header=dict(
                values=["<b>Table</b>", "<b>SLO Status</b>"],
                fill_color="#4C78A8",
                font=dict(color="white", size=13),
                align="center",
            ),
            cells=dict(
                values=[tables, slo_statuses],
                fill_color=[["white"] * len(tables), slo_colors],
                font=dict(size=12),
                align="center",
            ),
        ),
        row=3, col=1,
    )

    fig.update_layout(
        title_text=f"Travel Data Migration Pipeline Report — {run_timestamp}",
        title_font_size=18,
        height=950,
        template="plotly_white",
    )

    # Save to monitoring/ directory
    os.makedirs("monitoring", exist_ok=True)
    out_path = os.path.join("monitoring", f"report_{run_timestamp}.html")
    fig.write_html(out_path, include_plotlyjs="cdn")

    print(f"[dashboard] Saved → {out_path}")
    return out_path
