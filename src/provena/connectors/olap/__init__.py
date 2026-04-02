"""OLAP connectors — analytical / aggregation paradigm."""

from provena.connectors.olap.base import BaseOLAPConnector
from provena.connectors.olap.generic import GenericOLAPConnector

__all__ = [
    "BaseOLAPConnector",
    "GenericOLAPConnector",
]
