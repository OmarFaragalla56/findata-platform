"""LLM wrapper and prompts shared by the self_heal and triage nodes.

`client.py` always returns schema-validated structured output (a Pydantic model),
never free-text the caller has to parse.
"""
