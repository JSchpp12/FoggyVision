"""FoggyVision: synthetic fog imagery, renderer metadata, and dataset tooling.

Importing the top-level package is deliberately lightweight: it loads only this
module.  Dataset/ML helpers such as :mod:`fogvis.dataset` import torch and must
be imported explicitly.

Dataset import, query, cleanup, and rebuild live in the separate
``foggy_vision_loader`` package (``fogvis-cli``); this package depends on it for
the shared database layer.
"""

__version__ = "0.3.0"
