# coding=utf-8

import pandas as pd
from suanpan import path, runtime
from suanpan.components import Result
from suanpan.error import AppError
from suanpan.storage import storage
from suanpan.storage.arguments import Data

#from dw.sqlite import SqliteDataWarehouse
from utils import io


class Csv(Data):
    FILETYPE = "csv"

    def transform(self, value):
        filePath = super().transform(value)
        if not filePath:
            return None
        _load = io.loadCsv if self.required else runtime.saferun(io.loadCsv)
        return _load(filePath)

    def save(self, result):
        path.mkdirs(self.filePath, parent=True)
        io.dumpCsv(result.value, self.filePath)
        return super().save(Result.froms(value=self.filePath))


class Hdf5(Data):
    FILETYPE = "hdf5"

    def transform(self, value):
        filePath = super().transform(value)
        if not filePath:
            return None
        _load = io.loadH5 if self.required else runtime.saferun(io.loadH5)
        return _load(filePath)

    def save(self, result):
        path.mkdirs(self.filePath, parent=True)
        io.dumpH5(result.value, self.filePath)
        return super().save(Result.froms(value=self.filePath))


class Sqlite(Data):
    FILETYPE = "sqlite"

    def createSqliteHander(self, database):
        return SqliteDataWarehouse(database)

    def transform(self, value):
        filePath = super().transform(value)
        if not filePath:
            return None
        _create = (
            self.createSqliteHander
            if self.required
            else runtime.saferun(self.createSqliteHander)
        )
        return _create(filePath)

    def save(self, result):
        path.mkdirs(self.filePath, parent=True)
        handler = SqliteDataWarehouse(self.filePath)

        if isinstance(result.value, pd.DataFrame):
            handler.writeTable("data", result.value)
        elif isinstance(result.value, dict):
            handler.writeTables(result.value)
        else:
            raise AppError(f"Unknown result type: {type(result.value)}")

        return super().save(Result.froms(value=self.filePath))


class Parquet(Data):
    FILETYPE = "parquet"

    def transform(self, value):
        filePath = super().transform(value)
        if not filePath:
            return None
        _load = io.loadParquet if self.required else runtime.saferun(io.loadParquet)
        return _load(filePath)

    def save(self, result):
        path.mkdirs(self.filePath, parent=True)
        io.dumpParquet(result.value, self.filePath)
        return super().save(Result.froms(value=self.filePath))


class Excel(Data):
    FILETYPE = "xlsx"
    LEGACY_FILETYPE = "xls"

    def __init__(self, key, **kwargs):
        self.fileTypeSet = kwargs.get("type") is not None
        super().__init__(key, **kwargs)

    def load(self, args):
        super().load(args)
        if not self.objectName:
            self.value = self.filePath
            return self
        self.filePath = storage.getPathInTempStore(self.objectName)
        path.safeMkdirsForFile(self.filePath)
        self.value = self.filePath
        return self

    def transform(self, value):
        if not storage.isFile(self.objectName) and not self.fileTypeSet:
            fileType = self.LEGACY_FILETYPE
            self.fileName = "{}.{}".format(self.fileName.split(".")[0], fileType)
            self.objectName = storage.storagePathJoin(self.objectPrefix, self.fileName)
        self.filePath = storage.getPathInTempStore(self.objectName)
        value = super().transform(value)
        if self.filePath:
            _load = io.loadExcel if self.required else runtime.saferun(io.loadExcel)
            value = _load(self.filePath)
        return value

    def save(self, result):
        path.mkdirs(self.filePath, parent=True)
        io.dumpExcel(result.value, self.filePath)
        return super().save(Result.froms(value=self.filePath))
