# Helper functions for search and retrieval of elsevier developers api.

from elsapy import elsclient
from elsapy import log_util
from elsapy.elsdoc import ElsAbstract, ElsSerial
from elsapy.elsprofile import ElsAuthor, ElsAffil
from elsapy.elssearch import ElsSearch, ElsSerialTitleSearch

from etl.config import config

logger = log_util.get_logger(__name__)
scopus_api_key = config.SCOPUS_APIKEY


def get_docs_by_year(year, affl_id='60005248', get_all=False, api_key=scopus_api_key):
    """
    Get documents by year

    :param year:
    :param affl_id:
    :param get_all:
    :param api_key:
    :return:
    """
    client = elsclient.ElsClient(api_key)

    # Split the search since for recent years JHU has publications more than 5,000 each year.
    search_one = ElsSearch(f'af-id({affl_id})', 'scopus', {'date': year})
    search_one.execute(client, get_all)
    all_results = search_one.results
    #
    # search_two = ElsSearch('af-id(60005248) AND NOT subjarea(MEDI)', 'scopus', {'date': year})
    # search_two.execute(client, get_all)
    # all_results += search_two.results

    return all_results


def get_docs_by_author(author_id, api_key=scopus_api_key):
    """
    Get documents published by an author

    :param author_id:
    :param api_key:
    :return:
    """
    client = elsclient.ElsClient(api_key)
    search = ElsSearch(f'au-id({author_id})', 'scopus')
    search.execute(client, True)
    for doc in search.results:
        yield doc


def get_document(doc_id, api_key=scopus_api_key):
    """
    Retrieves a document

    :param doc_id:
    :param api_key:
    :return:
    """
    client = elsclient.ElsClient(api_key)
    doc = ElsAbstract(scopus_id=doc_id)
    doc.read(client)
    doc_data = doc.data
    doc_data['_id'] = doc_data['coredata']['dc:identifier'].split(':')[1]
    refs = get_doc_refs(doc_data['_id'])
    if 'affiliation' in doc_data and isinstance(doc_data['affiliation'], dict):
        doc_data['affiliation'] = [doc_data['affiliation']]
    if refs:
        refs_data = refs.get('references', {}).get('reference', [])
        if isinstance(refs_data, list):
            doc_data['references'] = refs_data
        elif isinstance(refs_data, dict):  # Fix the reference data to always make it an array
            doc_data['references'] = [refs_data]
    return doc_data


def get_doc_refs(doc_id, api_key=scopus_api_key):
    """
    Retrieves references of a document

    :param doc_id:
    :param api_key:
    :return:
    """
    client = elsclient.ElsClient(api_key)
    refs = ElsAbstract(scopus_id=doc_id, params={'view': 'REF'})
    refs.read(client)
    return refs.data


def get_authors_from_doc(doc, api_key=scopus_api_key):
    for author in doc.get('authors', {}).get('author', []):
        yield author
        # affl = author['affiliation']
        # if isinstance(affl, list):
        #     if next(a for a in affl if a['@id'] == '6005248'):
        #         yield author
        #     for a in affl:
        #         if a['@id'] == '6005248':
        #             yield author
        #             break
        # else:
        #     if affl['@id'] == '60005248':
        #         yield author


def get_doc_authors(doc_id, api_key=scopus_api_key):
    """
    Retrieves document-author relationship in tuples

    :param doc_id:
    :param api_key:
    :return:
    """
    """"""
    client = elsclient.ElsClient(api_key)
    abstract = ElsAbstract(scopus_id=doc_id, params={'field': 'author,affiliation'})
    abstract.read(client)
    for author in abstract.authors:
        yield {'doc': doc_id, 'author': author.get('@auid', None), 'seq': author.get('@seq', None)}


def get_author(author_id, api_key=scopus_api_key):
    """
    Retrieves an author

    :param author_id:
    :param api_key:
    :return:
    """
    client = elsclient.ElsClient(api_key)
    author = ElsAuthor(author_id=author_id)
    if not author.read(client):
        logger.error("[!]Read author failed: %s", author_id)
    author.data['_id'] = author.data['coredata']['dc:identifier'].split(':')[1]
    return author.data


def get_author_affl(author_data):
    author_id = author_data['coredata']['dc:identifier']
    author_id = author_id[author_id.find(':') + 1:]
    if 'affiliation-history' in author_data:
        affl_history = author_data.get('affiliation-history', {}).get('affiliation', {})
        if isinstance(affl_history, dict):
            yield {'author': author_id, 'affiliation': affl_history['@id'], 'seq': 1}
        else:
            for idx, val in enumerate(affl_history):
                yield {'author': author_id, 'affiliation': val['@id'], 'seq': idx+1}
    else:
        affl_history = author_data['affiliation-current']
        yield {'author': author_id, 'affiliation': affl_history['@id'], 'seq': 1}


def get_affiliation(affl_id, api_key=scopus_api_key):
    client = elsclient.ElsClient(api_key)
    affl = ElsAffil(affil_id=affl_id)
    if not affl.read(client):
        logger.error("[!]Read affiliation failed: %s", affl_id)
    affl.data['_id'] = affl.data['coredata']['dc:identifier'].split(':')[1]
    yield affl.data
    # retrieve ancestors
    # parent_id = affl.parent
    # while parent_id is not None:
    #     parent = ElsAffil(affil_id=parent_id)
    #     if not parent.read(client):
    #         logger.error("[!]Read affiliation failed: %s", parent_id)
    #     parent.data['_id'] = parent.data['coredata']['dc:identifier'].split(':')[1]
    #     yield parent.data
    #     parent_id = parent.parent


def get_serial(serial_id, api_key=scopus_api_key):
    """
    Retrieves a serial by a scopus ID
    :param serial_id:
    :param api_key:
    :return:
    """
    logger.info('getting serial: ' + serial_id)
    client = elsclient.ElsClient(api_key)
    serial = ElsSerial(scopus_id=serial_id, params={'view': 'STANDARD'})
    if not serial.read(client):
        logger.error("[!]Read serial failed: %s", serial_id)
    if 'error' in serial.data:
        logger.error("[!]Read serial with error: %s, %s", serial.data['error'], serial_id)
    else:
        data = serial.data['entry'][0]
        data['_id'] = data['source-id']
        return data


def get_serial_by_title(title, api_key=scopus_api_key):
    """
    Retrieves serials by the title

    :param title:
    :param api_key:
    :return:
    """
    logger.info('searching serial by title: ' + title)
    client = elsclient.ElsClient(api_key)
    search = ElsSerialTitleSearch(title)
    search.execute(client)
    for serial in search.results:
        serial['_id'] = serial['source-id']
        yield serial


def get_author_by_name(last_name, first_name, affiliation_id='60005248', api_key=scopus_api_key):
    """
    Search author by first name, last name and affiliation
    :param last_name:
    :param first_name:
    :param affiliation_id:
    :param api_key:
    :return:
    """
    logger.info('searching authors by name and affiliation')
    query = f'authlast({last_name}) and authfirst({first_name}) and af-id({affiliation_id})'
    client = elsclient.ElsClient(api_key)
    search = ElsSearch(query, 'author')
    search.execute(client)
    yield {'first_name': first_name, 'last_name': last_name, 'results': search.results}
