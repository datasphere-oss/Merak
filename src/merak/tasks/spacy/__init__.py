"""
This module contains a collection of tasks for interacting with the spaCy library.
"""
try:
    from merak.tasks.spacy.spacy_tasks import (
        SpacyNLP,
        SpacyTagger,
        SpacyParser,
        SpacyNER,
        SpacyComponent,
    )
except ImportError as exc:
    raise ImportError(
        'Using `merak.tasks.spacy` requires merak to be installed with the "spacy" extra.'
    ) from exc

__all__ = ["SpacyComponent", "SpacyNER", "SpacyNLP", "SpacyParser", "SpacyTagger"]
