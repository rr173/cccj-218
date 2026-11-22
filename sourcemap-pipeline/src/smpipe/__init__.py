"""Offline error-fact enrichment pipeline for compressed/translated builds.

The package never executes user scripts and never opens a network connection.
All artefacts, source maps and source bodies are taken from a local build
manifest and stored in a re-openable on-disk store.
"""

__all__ = ["errors", "vlq", "columns", "sourcemap", "store", "pipeline",
           "facts", "reverse"]
