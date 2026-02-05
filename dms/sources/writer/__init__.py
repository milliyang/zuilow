"""
Data Writers

Write OHLCV to databases; abstract base Writer and InfluxDB 1.x implementation (single/multi-server).

Classes:
    Writer           Abstract base: connect, disconnect, write_data, get_latest_date, write_data_incremental
    InfluxDBWriter   InfluxDB 1.x writer; single server or multi-server (write to all, primary = fastest)

Exports:
    Writer, InfluxDBWriter
"""

from .base import Writer
from .influxdb_writer import InfluxDBWriter

__all__ = [
    "Writer",
    "InfluxDBWriter",
]
