"""Builds the SqliteSaver checkpointer and manages thread ids.

A run's thread_id is generated once (new_thread_id) and reused verbatim for any
later Command(resume=...) call against that same run — never issue a new
thread_id to resume, or the checkpointer won't find the interrupted state.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager

from langgraph.checkpoint.sqlite import SqliteSaver

from pipeline_doctor.config.settings import get_settings


def new_thread_id() -> str:
    """A fresh thread_id for a new run."""
    return str(uuid.uuid4())


@contextmanager
def get_checkpointer():
    """Yields a SqliteSaver pointed at the configured checkpoint db path,
    creating the parent directory if needed."""
    db_path = get_settings().checkpoint_db
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with SqliteSaver.from_conn_string(db_path) as saver:
        yield saver


def thread_config(thread_id: str) -> dict:
    """The LangGraph config dict for invoking/resuming a given thread."""
    return {"configurable": {"thread_id": thread_id}}
