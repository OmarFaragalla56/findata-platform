"""Thin LLM wrapper shared by self_heal and triage. Always returns
schema-validated structured output (a Pydantic model), never free-text the
caller has to parse.

Uses langchain-anthropic's ChatAnthropic + with_structured_output, which for
Claude is implemented via a forced tool call: the response is guaranteed to
validate against the given schema or raise, so callers never handle a
malformed-JSON case themselves.
"""

from __future__ import annotations

import os

from langchain_anthropic import ChatAnthropic

from pipeline_doctor.config.settings import get_settings


def get_llm(schema):
    """Returns a chat model bound to `schema` for structured output — call
    `.invoke(messages)` on the result and get back a validated instance of
    `schema` directly."""
    settings = get_settings()
    model = ChatAnthropic(model=settings.llm_model, temperature=0)
    return model.with_structured_output(schema)


def llm_available() -> bool:
    """Whether an LLM call can be made at all.

    Nodes check this instead of letting ChatAnthropic raise. Without a key,
    triage and self_heal used to fail the whole run — fine for a developer
    running the CLI by hand, but this is meant to run unattended on a schedule,
    where a missing or expired key should degrade the decision quality, not
    take the pipeline's monitoring offline.
    """
    return bool(os.getenv("ANTHROPIC_API_KEY"))
