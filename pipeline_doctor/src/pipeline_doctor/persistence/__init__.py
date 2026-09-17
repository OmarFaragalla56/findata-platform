"""Storage layer: LangGraph checkpointing (`checkpointer.py`) and data-quality
baseline snapshots (`baseline_store.py`).

These are deliberately two separate stores with different lifecycles — see the
"Baseline storage decision" section of the build plan for why baselines are not
kept inside the checkpoint database.
"""
