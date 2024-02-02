import os
import importlib

import confuse


def get_config(config_fpath):
    if not os.path.exists(config_fpath):
        raise IOError(f'"{config_fpath}" does not exist!')

    config = confuse.Configuration("EnviroData")

    config.set_file(config_fpath)

    return config.get()


def load_object(modulename, objname):
    try:
        module = importlib.import_module(modulename)
    except:
        raise IOError(f"Importing module {modulename} failed.")

    try:
        obj = getattr(module, objname)
    except:
        raise IOError(f"Loading object {objname} from {modulename} failed.")

    return obj
