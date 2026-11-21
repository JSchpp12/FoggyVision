"""FoggyVision: synthetic fog imagery, renderer metadata, and dataset tooling.

Importing the top-level package is deliberately lightweight: it loads only this
module.  Dataset/ML helpers such as :mod:`fogvis.dataset` import torch and must
be imported explicitly.

Query and browse datasets with the separate ``foggy_vision_loader`` package.
"""

__version__ = "0.1.8"
