"""Configuration singleton for YAML config with merge and environment overrides.

The :mod:`redup_servicekit.config` module contains:

- :class:`redup_servicekit.config.ConfigSingleton`
"""

__docformat__ = 'restructuredtext'

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml


class ConfigSingleton:
    r"""Singleton for loading and merging YAML configuration.

    Supports deep merge of additional YAML files or dicts, and override of
    config values from environment variables. Use ``___`` (triple underscore)
    in env names for nesting; use ``__`` (double) for spaces in key names.
    Comma-separated env values become lists.

    Example:

    >>> from pathlib import Path
    >>> from redup_servicekit.config import ConfigSingleton
    >>> ConfigSingleton.load(Path("/config/config.yaml"))
    >>> ConfigSingleton.merge(Path("/config/config.local.yaml"))  # if exists
    >>> ConfigSingleton.inject_os_envs()
    >>> config = ConfigSingleton.get()
    """

    _data: Optional[Dict[str, Any]] = None

    @staticmethod
    def _deep_merge(source: Dict[str, Any], target: Dict[str, Any]) -> Dict[str, Any]:
        for config_key, config_value in source.items():
            if (
                isinstance(config_value, dict)
                and config_key in target
                and isinstance(target[config_key], dict)
            ):
                ConfigSingleton._deep_merge(config_value, target[config_key])
            else:
                target[config_key] = config_value
        return target

    @classmethod
    def load(cls, config_path: Union[str, Path]) -> Dict[str, Any]:
        r"""Load configuration from a YAML file.

        :param config_path: Path to a ``.yaml`` file.
        :type config_path: str or path-like
        :return: The loaded config as a dict.
        :rtype: dict
        :raises ValueError: If the path does not exist or is not a ``.yaml`` file.
        """
        config_path_resolved = Path(config_path)
        if not config_path_resolved.exists():
            raise ValueError(
                "The path to the configuration file does not exist.")
        if config_path_resolved.suffix != ".yaml":
            raise ValueError(
                "The path to the configuration file must be specified as a .yaml file."
            )
        with config_path_resolved.open() as config_file:
            cls._data = yaml.load(config_file, Loader=yaml.Loader)
        return cls._data

    @classmethod
    def get(cls) -> Dict[str, Any]:
        r"""Return the current configuration dict.

        :return: The config dict.
        :rtype: dict
        :raises ValueError: If configuration has not been loaded.
        """
        if cls._data is None:
            raise ValueError("Configuration not initialized.")
        return cls._data

    @classmethod
    def merge(cls, config: Union[str, Path, Dict[str, Any]]) -> None:
        r"""Merge another YAML file or dict into the current config (deep merge).

        :param config: Path to a YAML file or a dict to merge in.
        :type config: str, path-like, or dict
        :raises ValueError: If config not initialized or path does not exist.
        """
        if cls._data is None:
            raise ValueError("Configuration not initialized.")
        if isinstance(config, dict):
            cls._deep_merge(config, cls._data)
            return
        merge_path = Path(config)
        if not merge_path.exists():
            raise ValueError(
                "The path to the merge configuration file does not exist and is not a dictionary."
            )
        with merge_path.open() as merge_config_file:
            extra = yaml.load(merge_config_file, Loader=yaml.Loader)
        cls._deep_merge(extra, cls._data)

    @classmethod
    def inject_os_envs(cls) -> None:
        r"""Override config values from environment variables.

        Env names like ``section___subsection___key`` map to nested keys;
        ``__`` in a segment is replaced by space in the key name.
        Comma-separated values become lists. Only keys already present
        in the config are overridden.
        """
        if cls._data is None:
            return
        changed = False
        for environment_variable_name in os.environ:
            path_segments = environment_variable_name.split("___")
            current_config_node = cls._data
            for path_segment in path_segments[:-1]:
                nested_key_with_spaces = path_segment.replace("__", " ")
                if nested_key_with_spaces not in current_config_node or not isinstance(
                    current_config_node.get(nested_key_with_spaces), dict
                ):
                    continue
                current_config_node = current_config_node[nested_key_with_spaces]
            leaf_config_key = path_segments[-1].replace("__", " ")
            if leaf_config_key not in current_config_node:
                continue
            environment_variable_value = os.environ[environment_variable_name]
            logging.info(
                "Changing value from config %s to environment value: %s.",
                repr(environment_variable_name),
                environment_variable_value,
            )
            current_config_node[leaf_config_key] = (
                environment_variable_value.split(",")
                if "," in environment_variable_value
                else environment_variable_value
            )
            changed = True
        if changed:
            logging.info("Current configuration: %s.", cls.get())
