# coding=utf-8

import os
import time

#import dask.dataframe as dd
import pandas as pd
import xlrd
from suanpan import g, path
from suanpan.log import logger
from suanpan.utils import csv, excel


def fileCheck(func):
    def wraper(file, *args, **kwargs):
        def getFileSize(filePath):
            return os.path.getsize(filePath)

        def getDirectorySize(dirPath):
            total_size = 0
            for directoryName, _, fileNames in os.walk(dirPath):
                for f in fileNames:
                    fp = os.path.join(directoryName, f)
                    # skip if it is symbolic link
                    if not os.path.islink(fp):
                        total_size += os.path.getsize(fp)

            return total_size

        def getSize(filePath, pathIsFile=False):
            size = getFileSize(filePath) if pathIsFile else getDirectorySize(filePath)
            return round(size / float(1024 * 1024), 2)

        pathIsFile = os.path.isfile(file)
        totalSize = getSize(file, pathIsFile)
        g.backendPandasEngine = totalSize < 2048
        if pathIsFile:
            logger.debug(f"Got File with {totalSize} MB.")
        else:
            logger.debug(f"Got directory with {totalSize} MB.")
        engine, compare = (
            ("pandas", "Smaller") if g.backendPandasEngine else ("dask", "Larger")
        )
        logger.debug(
            f"File size {compare} than 2GB, Use {engine} in data analysis component."
        )
        return func(file, *args, **kwargs)

    return wraper


@fileCheck
def loadCsv(file, *args, **kwargs):
    def _resetColumnIndex(df):
        if isinstance(df, pd.DataFrame):
            df = df.reset_index()
            df = df.drop(axis=1, columns=df.columns[0])
        else:
            indexColumn = df.index.name
            if not indexColumn:
                indexColumn = f"_index_{int(time.time())}"
            df[indexColumn] = 1
            df[indexColumn] = df[indexColumn].cumsum()
            df = df.set_index(indexColumn)
        return df

    def _loadPandas(file, *args, **kwargs):
        return csv.load(file, *args, **kwargs)

    def _loadDask(file, *args, **kwargs):
        df = dd.read_csv(file, *args, **kwargs)
        try:
            df = df.set_index(df.columns[0])
        except ValueError as e:
            logger.warning("Could not determine column types: {}".format(e))
            kwargs.setdefault("index_col", 0)
            df = (
                dd.from_pandas(pd.read_csv(file, *args, **kwargs), chunksize=5000)
                if not g.get("backendPandasEngine", True)
                else pd.read_csv(file, *args, **kwargs)
            )
        return df

    df = (
        _loadPandas(file, *args, **kwargs)
        if g.get("backendPandasEngine", True)
        else _loadDask(file, *args, **kwargs)
    )
    return _resetColumnIndex(df)


def dumpCsv(data, file, *args, **kwargs):
    path.safeMkdirsForFile(file)
    if isinstance(data, dd.DataFrame):
        kwargs.setdefault("single_file", True)

    data.to_csv(file, *args, **kwargs)
    return file


@fileCheck
def loadH5(file, *args, **kwargs):
    kwargs.setdefault("key", "data")
    readHdf = pd.read_hdf if g.get("backendPandasEngine", True) else dd.read_hdf
    return readHdf(file, *args, **kwargs)


def dumpH5(data, file, *args, **kwargs):
    kwargs.setdefault("key", "data")
    kwargs.setdefault("mode", "w")
    kwargs.setdefault("format", "table")

    path.safeMkdirsForFile(file)

    data.to_hdf(file, *args, **kwargs)

    return file


@fileCheck
def loadParquet(file, *args, **kwargs):
    readParquet = (
        pd.read_parquet if g.get("backendPandasEngine", True) else dd.read_parquet
    )
    return readParquet(file, *args, **kwargs)


def dumpParquet(data, file, *args, **kwargs):
    path.safeMkdirsForFile(file)

    data.to_parquet(file, *args, **kwargs)

    return file


@fileCheck
def loadOrc(file, *args, **kwargs):
    readOrc = pd.read_orc if g.get("backendPandasEngine", True) else dd.read_orc
    return readOrc(file, *args, **kwargs)


@fileCheck
def loadExcel(file, *args, **kwargs):
    kwargs.setdefault("sheet_name", 0)
    if g.get("backendPandasEngine", True):
        return excel.load(file, *args, **kwargs)

    return dd.from_pandas(excel.load(file, *args, **kwargs), chunksize=5000)


def dumpExcel(data, file, *args, **kwargs):
    if isinstance(data, dd.DataFrame):
        data = data.compute()

    return excel.dump(data, file, *args, **kwargs)


@fileCheck
def loadJson(file, *args, **kwargs):
    readJson = pd.read_json if g.get("backendPandasEngine", True) else dd.read_json
    return readJson(file, *args, **kwargs)


def dumpJson(data, file, *args, **kwargs):
    kwargs.setdefault("orient", "records")
    path.safeMkdirsForFile(file)

    data.to_json(file, *args, **kwargs)

    return file


def loadFile(filePath, subtype, *args, **kwargs):
    loadFuncMap = {
        "dataframe": loadCsv,
        "hdf5": loadH5,
        "excel": loadExcel,
        "parquet": loadParquet,
        "orc": loadOrc,
    }
    loadFunc = loadFuncMap.get(subtype, csv.load)

    return loadFunc(filePath, *args, **kwargs)


def getExcelSheetNames(excelFile):
    return xlrd.open_workbook(excelFile, on_demand=True).sheet_names()
