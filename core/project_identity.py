"""Canonical Mini AI Cloud package identity."""

from importlib.metadata import PackageNotFoundError, version

PROJECT_NAME = "mini-ai-cloud"
DEVELOPMENT_VERSION = "0.5.0"

try:
    PROJECT_VERSION = version(PROJECT_NAME)
except PackageNotFoundError:
    PROJECT_VERSION = DEVELOPMENT_VERSION
