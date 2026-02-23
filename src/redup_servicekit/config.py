import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml


class ConfigSingleton:
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
        if cls._data is None:
            raise ValueError("Configuration not initialized.")
        return cls._data

    @classmethod
    def merge(cls, config: Union[str, Path, Dict[str, Any]]) -> None:
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
