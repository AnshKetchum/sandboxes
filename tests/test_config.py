"""get_sandbox_provider() resolution: config_path > SANDBOX_CONFIG_PATH env >
docker default; sandbox_providers[0] parsing; unsupported name handling."""

from __future__ import annotations

import pytest
import yaml

from sandboxes import DockerProvider, get_sandbox_provider
from sandboxes.errors import ConfigError


def test_no_config_at_all_defaults_to_docker(monkeypatch):
    monkeypatch.delenv("SANDBOX_CONFIG_PATH", raising=False)
    provider = get_sandbox_provider()
    assert isinstance(provider, DockerProvider)
    assert provider.config.name == "docker"


def test_env_var_path_is_used_when_no_explicit_path(tmp_path, monkeypatch):
    cfg = tmp_path / "sandbox.yaml"
    cfg.write_text(yaml.safe_dump({"sandbox_providers": [{"name": "docker", "image": "alpine"}]}))
    monkeypatch.setenv("SANDBOX_CONFIG_PATH", str(cfg))
    provider = get_sandbox_provider()
    assert provider.config.image == "alpine"


def test_explicit_path_wins_over_env_var(tmp_path, monkeypatch):
    env_cfg = tmp_path / "env.yaml"
    env_cfg.write_text(yaml.safe_dump({"sandbox_providers": [{"name": "docker", "image": "from-env"}]}))
    monkeypatch.setenv("SANDBOX_CONFIG_PATH", str(env_cfg))

    explicit_cfg = tmp_path / "explicit.yaml"
    explicit_cfg.write_text(yaml.safe_dump({"sandbox_providers": [{"name": "docker", "image": "from-explicit"}]}))

    provider = get_sandbox_provider(explicit_cfg)
    assert provider.config.image == "from-explicit"


def test_daytona_shaped_config_parses_every_field(tmp_path):
    cfg = tmp_path / "sandbox.yaml"
    cfg.write_text(yaml.safe_dump({
        "sandbox_providers": [{
            "name": "daytona",
            "disk_gb": 8,
            "cpu": 1,
            "memory_mb": 2048,
            "peak_concurrent_sandboxes": 230,
            "spend_cap_usd": 2500,
            "auto_stop_minutes": 120,
            "auto_delete_minutes": 0,
        }],
    }))
    provider = get_sandbox_provider(cfg)
    assert provider.name == "daytona"
    assert provider.config.disk_gb == 8
    assert provider.config.cpu == 1
    assert provider.config.memory_mb == 2048
    assert provider.config.peak_concurrent_sandboxes == 230
    assert provider.config.spend_cap_usd == 2500
    assert provider.config.auto_stop_minutes == 120
    assert provider.config.auto_delete_minutes == 0


def test_empty_sandbox_providers_list_defaults_to_docker(tmp_path):
    cfg = tmp_path / "sandbox.yaml"
    cfg.write_text(yaml.safe_dump({"sandbox_providers": []}))
    provider = get_sandbox_provider(cfg)
    assert isinstance(provider, DockerProvider)


def test_missing_sandbox_providers_key_defaults_to_docker(tmp_path):
    cfg = tmp_path / "sandbox.yaml"
    cfg.write_text(yaml.safe_dump({"unrelated": "stuff"}))
    provider = get_sandbox_provider(cfg)
    assert isinstance(provider, DockerProvider)


def test_unsupported_provider_name_raises_clearly(tmp_path):
    cfg = tmp_path / "sandbox.yaml"
    cfg.write_text(yaml.safe_dump({"sandbox_providers": [{"name": "ec2"}]}))
    with pytest.raises(ConfigError, match="unsupported sandbox provider"):
        get_sandbox_provider(cfg)


def test_missing_config_file_raises_clearly(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        get_sandbox_provider(tmp_path / "nope.yaml")


def test_bad_yaml_raises_clearly(tmp_path):
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("sandbox_providers: [unclosed")
    with pytest.raises(ConfigError):
        get_sandbox_provider(cfg)


def test_only_the_first_provider_entry_is_used(tmp_path):
    cfg = tmp_path / "sandbox.yaml"
    cfg.write_text(yaml.safe_dump({"sandbox_providers": [
        {"name": "docker", "image": "first"},
        {"name": "daytona", "image": "second"},
    ]}))
    provider = get_sandbox_provider(cfg)
    assert isinstance(provider, DockerProvider)
    assert provider.config.image == "first"
