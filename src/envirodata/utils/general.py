"""Envirodata utilities."""

from typing import Callable

import os
import importlib
import subprocess
from argparse import ArgumentParser

import confuse  # type: ignore

import logging

logger = logging.getLogger(__name__)


def get_cli_arguments():
    def valid_file_path(fp: str):
        if not os.path.exists(fp):
            raise IOError(f"Config file does not exist at {fp}")

        return fp

    def valid_list(input_string):
        try:
            return input_string.split(",")
        except ValueError as exc:
            raise ValueError(f"Not a valid list of services: {input_string}") from exc

    parser = ArgumentParser("EnviroData")
    parser.add_argument("config_file", type=valid_file_path)
    parser.add_argument("--services", type=valid_list, default=None)

    args = parser.parse_args()

    return args


def get_config(config_fpath: str) -> confuse.Configuration:
    """Read configuration from file

    :param config_fpath: path to configuration file (.yaml)
    :type config_fpath: str
    :raises IOError: path does not exist!
    :return: Configuration object
    :rtype: confuse.Configuration
    """
    if not os.path.exists(config_fpath):
        raise IOError(f'"{config_fpath}" does not exist!')

    config = confuse.Configuration("EnviroData")

    config.set_file(config_fpath)

    return config.get()


def load_object(modulename: str, objname: str) -> object:
    """Load python object identified by its
    module and name.

    :param modulename: Name of the module to search in
    :type modulename: str
    :param objname: Name of the object
    :type objname: str
    :raises IOError: Could not import module
    :raises IOError: Could not load object from module
    :return: Object
    :rtype: object
    """
    try:
        module = importlib.import_module(modulename)
    except Exception as exc:
        raise IOError(f"Importing module {modulename} failed.") from exc

    try:
        obj = getattr(module, objname)
    except Exception as exc:
        raise IOError(f"Loading object {objname} from {modulename} failed.") from exc

    return obj


def load_callable(modulename: str, objname: str) -> Callable:
    """Load python callable (class, method, ...) identified by its
    module and name.

    :param modulename: Name of the module to search in
    :type modulename: str
    :param objname: Name of the callable object
    :type objname: str
    :raises IOError: Loaded object is not callable
    :return: Callable
    :rtype: Callable
    """
    obj = load_object(modulename, objname)

    if not callable(obj):
        raise IOError(f"Object {objname} from {modulename} is not callable.")

    return obj


def get_git_commit_hash():
    """Retrieve the current git commit hash for the project.

    :return: git commit hash or "unknown"
    :rtype: str
    """
    try:
        full_hash = subprocess.check_output(["git", "rev-parse", "HEAD"])
        full_hash = str(full_hash, "utf-8").strip()
        return full_hash
    except subprocess.CalledProcessError as exc:
        logger.critical("Could not determine git commit hash: %s", str(exc))
        return "unknown"
