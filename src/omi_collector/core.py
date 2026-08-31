from importlib.metadata import version


def package_version() -> str:
    return version("omi-collector")
