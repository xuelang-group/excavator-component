# coding=utf-8

import pandas as pd
from suanpan.arguments import Json as BaseJson
from suanpan.utils import json

from args import base

Csv = base.Csv
Hdf5 = base.Hdf5
Sqlite = base.Sqlite
Parquet = base.Parquet
Excel = base.Excel


class Json(BaseJson):
    def transform(self, value):
        value = super().transform(value)
        if isinstance(value, list) or isinstance(value[list(value.keys())[0]], list):
            return pd.DataFrame(value)

        return pd.DataFrame([value])

    def save(self, result):
        self.logSaved(result.value)
        return json.dumps(result.value.to_dict("records"))
