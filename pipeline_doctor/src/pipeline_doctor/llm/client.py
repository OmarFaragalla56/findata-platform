"""Thin LLM wrapper shared by self_heal and triage. Always returns
schema-validated structured output (a Pydantic model), never free-text the
caller has to parse.

Uses langchain-anthropic's ChatAnthropic + with_structured_output, which for
Claude is implemented via a forced tool call: the response is guaranteed to
validate against the given schema or raise, so callers never handle a
malformed-JSON case themselves.
"""

from __future__ import annotations

from langchain_anthropic import ChatAnthropic

from pipeline_doctor.config.settings import get_settings


def get_llm(schema):
    """Returns a chat model bound to `schema` for structured output — call
    `.invoke(messages)` on the result and get back a validated instance of
    `schema` directly."""
    settings = get_settings()
    model = ChatAnthropic(model=settings.llm_model, temperature=0)
    return model.with_structured_output(schema)
