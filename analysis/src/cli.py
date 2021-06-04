"""Main `scitrend-analysis` cli"""
import click
import logging
import sys
import torch

from pathlib import Path
from src import __version__
from src.data import neurips


def version_msg() -> str:
    """Returns the package version, location, Python version"""
    python_version = sys.version[:3]
    location = Path(__file__).resolve().as_posix()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    if "cuda" in device.type:
        device_properties = torch.cuda.get_device_properties(device)
        device_message = f"{device_properties.name} ({device_properties.total_memory}, "
        f"{device_properties.major}.{device_properties.minor}, "
        f"{device_properties.multi_processor_count} multi-processors)"
    else:
        device_message = "None. Using CPU."
    message = [f"Scitrend analysis at {location}"]
    message.append(f"Python version: {python_version}")
    message.append(f"Accelerator: {device_message}")
    return "\n".join(message)


def main():
    pass


if __name__ == "__main__":
    main()
