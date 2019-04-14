# Usage: pipenv run bonobo run -m etl.jobs.scopus.biophysics

import os
import sys
import bonobo
from pymongo import MongoClient
from etl.api.elsapi import *
from etl.utils.transformers import *
from bonobo.constants import NOT_MODIFIED
from etl.config import config, env

database = config.MONGO_DATABASE
limit = 1 if env == 'test' else sys.maxsize


def extract_authors(args):
    for author in args['results']:
        yield author


def create_author_document(args):
    data = {
        'first_name': args['first_name'],
        'last_name': args['last_name'],
        'scopus_ids': [extract_id(x) for x in args['results']]
    }
    yield data


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()

    # Read data from the CSV file and load into MongoDB
    graph.add_chain(
        bonobo.CsvReader('data/biophysics-author-names.csv'),
        bonobo.Limit(limit),
        get_author_by_name,
        create_author_document,
        MongoWriter(collection='jhu-authors', database=database),
    )

    # Extract authors from Scopus and load into MongoDB
    graph.add_chain(
        extract_authors,
        bonobo.Limit(limit),
        extract_id,
        FilterDuplicate(collection='scopus-authors', database=database),
        get_author,
        MongoWriter(collection='scopus-authors', database=database),
        _input=get_author_by_name
    )

    # Extract documents from Scopus and load into MongoDB
    graph.add_chain(
        extract_id,
        get_docs_by_author,
        bonobo.Limit(limit),
        extract_id,
        FilterDuplicate(collection='scopus-documents', field='_id', database=database),
        get_document,
        remove_errata,
        MongoWriter(collection='scopus-documents', database=database),
        _input=extract_authors
    )

    # Extract serials data from Scopus and load into MongoDB
    graph.add_chain(
        lambda args: args['coredata'].get('source-id', None),
        bonobo.Limit(limit),
        FilterDuplicate(collection='scopus-serials', database=database),
        get_serial,
        MongoWriter(collection='scopus-serials', database=database),
        _input=remove_errata
    )

    # Extract co-authors data from Scopus and load into MongoDB
    graph.add_chain(
        get_authors_from_doc,
        bonobo.Limit(limit),
        FilterDuplicate(collection='scopus-authors', field='@auid', database=database),
        lambda args: args['@auid'],
        get_author,
        MongoWriter(collection='scopus-authors', database=database),
        # bonobo.JsonWriter('results/authors.json'),
        _input=remove_errata
    )

    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """
    return {
        # 'mongodb.client': MongoClient('mongo', 27017)
        'mongodb.client': MongoClient(config.MONGO_HOST, config.MONGO_PORT)
    }


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()
    with bonobo.parse_args(parser) as options:
        bonobo.run(
            get_graph(**options),
            services=get_services(**options)
        )
