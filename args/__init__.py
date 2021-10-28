# coding=utf-8

from suanpan import app
from suanpan.imports import imports

Csv = imports(f"args.{app.TYPE}.Csv")
Hdf5 = imports(f"args.{app.TYPE}.Hdf5")
Sqlite = imports(f"args.{app.TYPE}.Sqlite")
Parquet = imports(f"args.{app.TYPE}.Parquet")
Excel = imports(f"args.{app.TYPE}.Excel")
Json = imports(f"args.{app.TYPE}.Json")
