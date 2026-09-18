"""Provider selection in llm.client. Every model class is replaced with a stub,
so these tests never construct a real client or make a request — they must be
safe to run with real keys present in the environment."""

from unittest.mock import MagicMock

import pytest

from pipeline_doctor.config.settings import DEFAULT_MODELS, get_settings
from pipeline_doctor.llm.client import get_llm, llm_available


@pytest.fixture
def stub_class():
    def make():
        instance = MagicMock()
        cls = MagicMock(return_value=instance)
        return cls, instance
    return make


def test_anthropic_is_the_default(monkeypatch, stub_class):
    monkeypatch.delenv("PIPELINE_DOCTOR_LLM_PROVIDER", raising=False)
    monkeypatch.delenv("PIPELINE_DOCTOR_LLM_MODEL", raising=False)
    cls, instance = stub_class()
    monkeypatch.setattr("langchain_anthropic.ChatAnthropic", cls)

    get_llm(dict)

    cls.assert_called_once_with(model=DEFAULT_MODELS["anthropic"], temperature=0)
    instance.with_structured_output.assert_called_once_with(dict)


def test_google_provider_builds_gemini_with_no_hidden_retries(monkeypatch, stub_class):
    pytest.importorskip("langchain_google_genai")
    monkeypatch.setenv("PIPELINE_DOCTOR_LLM_PROVIDER", "google")
    monkeypatch.delenv("PIPELINE_DOCTOR_LLM_MODEL", raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "test-google-key")
    cls, instance = stub_class()
    monkeypatch.setattr("langchain_google_genai.ChatGoogleGenerativeAI", cls)

    get_llm(dict)

    kwargs = cls.call_args.kwargs
    assert kwargs["model"] == DEFAULT_MODELS["google"]
    assert kwargs["google_api_key"] == "test-google-key"
    # This library passes max_retries through as the SDK's total attempts, so 1
    # means exactly one request. Hidden retries are silent extra billed calls.
    assert kwargs["max_retries"] == 1
    instance.with_structured_output.assert_called_once_with(dict)


def test_switching_provider_switches_the_default_model(monkeypatch):
    monkeypatch.delenv("PIPELINE_DOCTOR_LLM_MODEL", raising=False)
    monkeypatch.setenv("PIPELINE_DOCTOR_LLM_PROVIDER", "google")
    assert get_settings().llm_model == DEFAULT_MODELS["google"]


def test_explicit_model_wins_over_the_provider_default(monkeypatch):
    monkeypatch.setenv("PIPELINE_DOCTOR_LLM_PROVIDER", "google")
    monkeypatch.setenv("PIPELINE_DOCTOR_LLM_MODEL", "some-other-gemini")
    assert get_settings().llm_model == "some-other-gemini"


def test_unknown_provider_is_rejected(monkeypatch):
    monkeypatch.setenv("PIPELINE_DOCTOR_LLM_PROVIDER", "carrier-pigeon")
    with pytest.raises(ValueError, match="unknown PIPELINE_DOCTOR_LLM_PROVIDER"):
        get_llm(dict)


@pytest.mark.parametrize("provider,key_var,other_var", [
    ("anthropic", "ANTHROPIC_API_KEY", "GOOGLE_API_KEY"),
    ("google", "GOOGLE_API_KEY", "ANTHROPIC_API_KEY"),
])
def test_llm_available_checks_the_configured_providers_key(monkeypatch, provider, key_var, other_var):
    monkeypatch.setenv("PIPELINE_DOCTOR_LLM_PROVIDER", provider)
    monkeypatch.delenv(key_var, raising=False)
    monkeypatch.setenv(other_var, "the-wrong-providers-key")
    assert llm_available() is False     # the other provider's key doesn't count

    monkeypatch.setenv(key_var, "right-key")
    assert llm_available() is True
