"""OLTP connectors — transactional / point-lookup paradigm."""

from provena.connectors.oltp.base import BaseOLTPConnector
from provena.connectors.oltp.generic import GenericOLTPConnector

__all__ = [
    "BaseOLTPConnector",
    "GenericOLTPConnector",
]
