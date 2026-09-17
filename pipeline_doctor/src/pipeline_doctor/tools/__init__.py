"""Deterministic helper functions used by nodes: running the target pipeline step,
profiling its output data, and diffing a profile against a stored baseline.

Everything here is a plain, independently unit-testable function — no LangGraph or
LLM dependency belongs in this package.
"""
