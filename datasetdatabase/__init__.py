#!/usr/bin/env python

import warnings
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")

from .core import LOCAL, DatasetDatabase, Dataset, read_dataset
