"""Document connectors — vector / semantic search paradigm."""

from provena.connectors.document.base import BaseDocumentConnector
from provena.connectors.document.generic import GenericDocumentConnector

__all__ = [
    "BaseDocumentConnector",
    "GenericDocumentConnector",
]
