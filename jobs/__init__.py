from .precipitation import Precipitation


def job(tbl, src, dest):
    if tbl == "precipitation":
        return Precipitation(tbl, src, dest)

    raise f"Unknown or unregistered table {tbl}"
