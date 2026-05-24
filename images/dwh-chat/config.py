"""Endpoint configuration loader for dwh-chat."""

from __future__ import annotations

import os
from dataclasses import dataclass, field

import yaml


@dataclass
class ModelConfig:
    id: str
    thinking: bool = False  # pass reasoning_content back (DeepSeek thinking mode)


@dataclass
class Endpoint:
    name: str
    base_url: str
    models: list[ModelConfig]
    api_key: str = ""


# profile_name → (Endpoint, ModelConfig)
_profiles: dict[str, tuple[Endpoint, ModelConfig]] = {}
_endpoints: list[Endpoint] = []


def _load() -> None:
    path = os.environ.get("ENDPOINTS_CONFIG", "/config/endpoints.yaml")
    with open(path) as f:
        raw = yaml.safe_load(f)

    for entry in raw:
        api_key = ""
        key_file = entry.get("apiKeyFile")
        if key_file:
            with open(key_file) as kf:
                api_key = kf.read().strip()

        models: list[ModelConfig] = []
        for m in entry["models"]:
            if isinstance(m, str):
                models.append(ModelConfig(id=m))
            else:
                models.append(ModelConfig(id=m["id"], thinking=m.get("thinking", False)))

        ep = Endpoint(
            name=entry["name"],
            base_url=entry["baseUrl"].rstrip("/"),
            models=models,
            api_key=api_key,
        )
        _endpoints.append(ep)
        for model_cfg in ep.models:
            profile_name = f"{ep.name} / {model_cfg.id}"
            _profiles[profile_name] = (ep, model_cfg)


# Load once at import time.
_load()


def get_profiles() -> dict[str, tuple[Endpoint, ModelConfig]]:
    """Return mapping of profile_name → (Endpoint, ModelConfig)."""
    return _profiles


def get_endpoints() -> list[Endpoint]:
    return _endpoints
