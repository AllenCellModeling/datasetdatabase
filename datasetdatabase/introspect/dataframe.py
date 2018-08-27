#!/usr/bin/env python

# installed
from typing import Dict, List
import pandas as pd

# self
from .introspector import Introspector
from ..utils import ProgressBar
from ..utils import checks

class DataFrameInspector(Introspector):
    def parse(self) -> Dict[str, List[int]]:
        # enforce types
        checks.check_types(self.obj, pd.DataFrame)

        all_ids = {}
        all_ids["Iota"] = []
        all_ids["Group"] = []
        all_ids["IotaGroup"] = []

        return all_ids
