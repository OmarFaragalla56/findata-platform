"""Thin LLM wrapper shared by self_heal and triage. Always returns
schema-validated structured output (a Pydantic model), never free-text the
caller has to parse.

Two providers, chosen by PIPELINE_DOCTOR_LLM_PROVIDER:

  anthropic (default)  ChatAnthropic,           key in ANTHROPIC_API_KEY
  google               ChatGoogleGenerativeAI,  key in GOOGLE_API_KEY

Both implement with_structured_output, so callers are provider-agnostic: they
get back a validated instance of the schema either way, and never handle a
malformed-JSON case themselves.
"""

from __future__ import annotations

import os

from pipeline_doctor.config.settings import get_settings

API_KEY_VARS = {
    "anthropic": "ANTHROPIC_API_KEY",
    "google": "GOOGLE_API_KEY",
}


def get_llm(schema):
    """Returns a chat model bound to `schema` for structured output — call
    `.invoke(messages)` on the result and get back a validated instance of
    `schema` directly."""
    settings = get_settings()
    provider = settings.llm_provider

    if provider == "anthropic":
        from langchain_anthropic import ChatAnthropic

        model = ChatAnthropic(model=settings.llm_model, temperature=0)
    elif provider == "google":
        # Optional dependency: `pip install pipeline-doctor[google]`.
        from langchain_google_genai import ChatGoogleGenerativeAI

        model = ChatGoogleGenerativeAI(
            model=settings.llm_model,
            temperature=0,
            google_api_key=os.environ[API_KEY_VARS["google"]],
            # Exactly one HTTP request per call. Despite the name, this library
            # passes max_retries straight through as the SDK's *total attempts*
            # (HttpRetryOptions(attempts=max_retries)), and the SDK's default is
            # 5 - each a billed request the caller never sees. 1 means one
            # attempt. 0 happens to work too, but only because google-genai
            # special-cases it (`if attempts == 0: attempts = 1`); the very next
            # line, `attempts or _RETRY_ATTEMPTS`, would otherwise turn 0 into 5.
            # Retries belong one level up, where self_heal decides them visibly.
            max_retries=1,
        )
    else:
        raise ValueError(
            f"unknown PIPELINE_DOCTOR_LLM_PROVIDER {provider!r}; expected one of {sorted(API_KEY_VARS)}"
        )

    return model.with_structured_output(schema)


def llm_available() -> bool:
    """Whether an LLM call can be made with the configured provider.

    Nodes check this instead of letting the client raise. Without a key,
    triage and self_heal used to fail the whole run — fine for a developer
    running the CLI by hand, but this is meant to run unattended on a schedule,
    where a missing or expired key should degrade the decision quality, not
    take the pipeline's monitoring offline.
    """
    key_var = API_KEY_VARS.get(get_settings().llm_provider)
    return bool(key_var and os.getenv(key_var))
