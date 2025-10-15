"""
OpenSky Pipeline Data Sources

This package contains custom Spark data sources for the OpenSky pipeline.
"""

from .opensky import OpenSkyDataSource
from .transcript import TranscriptDataSource

__all__ = ['OpenSkyDataSource', 'TranscriptDataSource']
