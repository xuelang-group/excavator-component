# coding=utf-8

import pandas as pd
from suanpan import path
from suanpan.components import Result
from suanpan.storage.arguments import Json as BaseJson
from suanpan.utils import json

from args import base

Csv = base.Csv
Hdf5 = base.Hdf5
Sqlite = base.Sqlite
Parquet = base.Parquet
Excel = base.Excel


class Json(BaseJson):
    def transform(self, value):
        result = super().transform(value)
        if not result:
            return result

        if isinstance(result, list) or isinstance(result[list(result.keys())[0]], list):
            return pd.DataFrame(result)

        return pd.DataFrame([result])

    def save(self, result):
        path.mkdirs(self.filePath, parent=True)
        json.dump(result.value.to_dict("records"), self.filePath)
        return super().save(Result.froms(value=self.filePath))
