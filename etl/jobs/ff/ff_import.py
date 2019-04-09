# Import faculty force data and scopus data based on FF

import sys
from datetime import datetime
import bonobo
from pymongo import MongoClient
from etl.api.elsapi import *
from etl.utils.transformers import *
from bonobo.constants import NOT_MODIFIED
from etl.config import config, env
from bonobo.config import use_raw_input

database = config.MONGO_DATABASE
limit = 1 if env == 'test' else sys.maxsize


def extract_authors(args):
    for author in args['results']:
        yield author


@use_raw_input
def create_author_document(row):
    # Bonobo CsvReader doesn't yield a dict by default.
    # See https://stackoverflow.com/questions/51673963/why-does-bonobos-csvreader-method-yield-tuples-and-not-dicts
    values = dict(zip(row._fields, row))
    if values['HireDate']:
        hire_date = datetime.strptime(values['HireDate'], '%Y-%m-%d')
    else:
        hire_date = None
    data = {
        'first_name': values['FirstName'],
        'last_name': values['LastName'],
        'name': values['Name'],
        'hopkins_id': values['HopkinsID'],
        'jhed_id': values['JHEDID'],
        'school': values['School'],
        'department': values['AcademicDepartment'],
        'org_unit': values['OrgUnit'],
        'role': values['Role'],
        'sap_rank': values['SAPRank'],
        'tenure_status': values['TenureStatus'],
        'gender': values['Gender'],
        'hire_date': hire_date,
        'data_source': values['Datasource'],
        'scopus_ids': values['scopusid'].split(';'),
        'last_modified': datetime.now(),
    }
    yield data


def get_graph(**options):
    graph = bonobo.Graph()

    # Import authors
    graph.add_chain(
        bonobo.CsvReader('data/ff-faculty.csv', skip=1),
        bonobo.Limit(limit),
        create_author_document,
        FilterDuplicate(collection="jhu-authors", field='jhed_id', target='jhed_id', database=database),
        MongoWriter(collection='jhu-authors', database=database),
    )

    return graph


def get_services(**options):
    return {
        'mongodb.client': MongoClient(config.MONGO_HOST, config.MONGO_PORT)
    }


if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(get_graph(**options), services=get_services(**options))
