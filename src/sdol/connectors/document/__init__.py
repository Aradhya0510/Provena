"""Document connectors — vector / semantic search paradigm."""

from sdol.connectors.document.base import BaseDocumentConnector
from sdol.connectors.document.generic import GenericDocumentConnector

__all__ = [
    "BaseDocumentConnector",
    "GenericDocumentConnector",
]
