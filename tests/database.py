import os
import re
import functools
# import inflection
import glob
import copy

from peewee import SqliteDatabase
from playhouse.reflection import generate_models
# from playhouse.reflection import print_model, print_table_sql
# from playhouse.shortcuts import model_to_dict, dict_to_model

from tests import settings

def get_database(db=':memory:'):
    db = SqliteDatabase(db)
    return db

def load_data(db):

    base_path = os.environ.get("DATAPORTAL_CLIENT_TEST_WCIB_DBSCRIPTS_PATH", "../wcib/deployment/wcib/dbscripts/")
    # base_path = "."
    if not os.path.isdir(base_path):
        raise Exception(f"Cant find wcib dbscripts path='{base_path}' please set env DATAPORTAL_CLIENT_TEST_WCIB_DBSCRIPTS_PATH to the right location")

    # dbs = [os.path.join(base_path, 'mysqlTables.sql'), os.path.join(base_path, 'mysqlschemamigs/v2.sql')]
    dbs = sorted(glob.glob(os.path.join(base_path, 'mysqlschemamigs/v*.sql')))

    def cleanSQL(sql):
        cleaners = [
                functools.partial(re.sub, r"\s+Comment\s+'[^']+',", ","),
                functools.partial(re.sub, r"\s+fileindexes\.", " "),
                functools.partial(re.sub, r".* POSSIBLEACCESSTYPES .*", "")  # Not suported in sqlight3
                ]

        for cl in cleaners:
            sql = cl(sql)
        return sql

    # JR: potential problem, sqlite don't support some alter table commands (i.e. add constraint in v3.sql) (https://www.sqlite.org/lang_altertable.html#otheralter)
    # JR: maybe run the dbs through SQL first and dump out the resulting database, or run towards deployment of wcib and skip all mocking?
    for f in dbs:
        with open(f, "r") as of:
            fc = of.read().split(";")
            for sql in fc:
                sql = cleanSQL(sql)
                print(f"exec: {sql}")
                # print(db.get_tables())
                db.execute_sql(sql)
                # print(db.get_tables())

    models = generate_models(db, literal_column_names=True)

    with db.atomic():
        for ds in settings.mockdatasets['Datasets']:
            ds_tags = ds['Tags']
            ds = copy.copy(ds)
            del ds['Tags']
            ds_id = models['Datasets'].insert(ds).execute()
            _ = models['Tags'].insert_many([{'DatasetID': ds_id, 'Tag': tag} for tag in ds_tags]).execute()
            # print(f"{ds_id}, {_}")

    with db.atomic():
        for key, dsfs in settings.mockfiles.items():
            for dsf in dsfs:
                # other raw data ?
                item = {"DatasetID": key, "RawData": 0, **dsf}
                # print(item)
                ds_id = models['DataFiles'].insert(item).execute()

    return models

#print([model_to_dict(a, backrefs=True) for a in list(models['DataFiles'].select().execute())])
#print([model_to_dict(a, backrefs=True) for a in list(models['Datasets'].select().join().execute())])
