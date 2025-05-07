import logging
import sys
from pathlib import Path
from typing import Optional


def check_level(level: int | str) -> int:
    """Check if the logging level is valid and return the corresponding integer value."""
    if isinstance(level, str):
        level_map = logging.getLevelNamesMapping()
        try:
            level = level_map[level.upper()]
        except KeyError:
            raise ValueError(f"Invalid logging level '{level}', must be one of {list(level_map.keys())}")
    if not isinstance(level, int):
        raise TypeError(f"Logging level must be an int or str, got {type(level)}")
    return level


def get_log_format(asctime=True, name=True, levelname=True) -> str:
    """Get the default log format string."""
    parts = []
    if asctime:
        parts.append("%(asctime)s")
    if name:
        parts.append("%(name)s")
    if levelname:
        parts.append("%(levelname)s")
    prefix = " - ".join(parts)
    if prefix:
        fmt = f"[{prefix}] "
    else:
        fmt = ""
    return fmt + "%(message)s"


def get_logger(
        name: Optional[str] = None,
        level: int | str = logging.WARNING,
        fmt: Optional[str] = None,
        path: Optional[str | Path] = None,
        stdout: Optional[bool] = None,
) -> logging.Logger:
    """Get a logger with the specified name and logging level."""
    level = check_level(level)
    logger = logging.getLogger(name)
    if not fmt:
        fmt = get_log_format()
    if path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path)
        file_handler.setLevel(level)
        formatter = logging.Formatter(fmt)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        if stdout is True:
            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setLevel(level)
            stdout_handler.setFormatter(formatter)
            logger.addHandler(stdout_handler)
    else:
        logging.basicConfig(level=level, format=fmt, stream=(sys.stdout if stdout else None))
    return logger
