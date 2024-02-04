"""Enivrodata utilities."""

import os
import importlib

import confuse


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
    """Load python object (class, method, ...) identified by its
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
