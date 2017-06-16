#!/usr/bin/python

"""
Utility functions.

"""


import time
import logging
from config import settings
from Authentication import Authentication


# API-kEY FOR UMLS REST TICKET SERVICES
umls_api = settings['api']['umls']
# UMLS REST SERVICES INITIALIZATION OF CLIENT AND TICKET
# GRANTING SERVICE TO BE USED IN ALL CASES
AuthClient = Authentication(umls_api)
tgt = AuthClient.gettgt()


def get_umls_ticket(apikey=None):
    """
    Get a single use ticket for the UMLS REST services.
    It is supposed that an Author Client and a Ticket
    Granting Service have already been set-up in case
    the apikey = None. If an api-key is given, create the
    above needed instances and generate a new ticket.
    Input:
        - apikey: str,
        UMLS REST services api-key. Default is None and
        the already establised service is used
    Output:
        - string of the generated ticket
    """

    # Get ticket from the already establised service
    if apikey is None:
        return AuthClient.getst(tgt)
    else:
        # Establish new Client and Ticket granting service
        AuthClient = Authentication(apikey)
        tgt = AuthClient.gettgt()
        return AuthClient.getst(tgt)


def time_log(phrase, time_start=None):
    """
    A time_logger function so as to print info with time since elapsed if wanted,
    alongside with the current logging config.
    """

    # If we want to also print time_elapsed
    if time_start:
        logging.info('%s in : %.2f seconds.' % (phrase, time.time() - time_start))
    else:
        logging.info('%s' % (phrase))
    return 1


def get_concept_from_source(source_id, source, apikey=None):
    """
    Function that maps an entity from another source to UMLS concepts.
    Input:
        - source_id: str,
        string of the unique id from the source knowledge base
        - source: str,
        string code-name of the knowledge base, as used by the
        UMLS REST services (e.g. drugbank -> DRUGBANK, MESH->MSH)
        - apikey: str,
        UMLS REST services api-key. Default is None and
        the already establised service is used. Check get_umls_ticket
        function for details
    Output:
        - concepts: list,
        list of dictionaries representing concepts mapped to the
        source_id entity. Each dictionary has keys "label", "cuid", "sem_types"
        Check get_concept_from_cui for more details
    """

    ticket = get_umls_ticket(apikey)
    params = {'string': source_id, 'sabs': source, 'searchType': 'exact',
              'inputType': 'sourceUi', 'ticket': ticket}
    url = "https://uts-ws.nlm.nih.gov/rest/search/current"
    r = requests.get(url, params=params)
    r.encoding = 'utf-8'
    items = json.loads(r.text)
    jsonData = items["result"]
    # Get cuis related to source_id
    cuis = [res['ui']for res in jsonData['results']]
    # Get concepts from cuis
    concepts = [get_concept_from_cui(cui, apikey) for cui in cuis]
    return concepts


def get_concept_from_cui(cui, apikey=None):
    """
    Function that fetches a concept's attributes from the corresponding cui.
    Input:
        - cui: str,
        string of cui that will be looked up
        - apikey: str,
        UMLS REST services api-key. Default is None and
        the already establised service is used. Check get_umls_ticket
        function for details
    Output:
        - res: dictionary,
        dictionary with the concepts attributes as fetched. Specifically,
        "label", "cuid"(cui) and "sem_types"(comma delimited string of
        the semantic types us returned)
    """

    ticket = get_umls_ticket(apikey)
    url = "https://uts-ws.nlm.nih.gov/rest/content/current/CUI/" + cui
    r = requests.get(url, params={'ticket': ticket})
    r.encoding = 'utf-8'
    items = json.loads(r.text)
    jsonData = items["result"]
    res = {'label': jsonData['name'], 'cuid': cui}
    sem_types = []
    # For each semantic type of the entity
    for stys in jsonData["semanticTypes"]:
        # Keep only the TUI code from the uri e.g.
        # https://uts-ws.nlm.nih.gov/rest/semantic-network/current/TUI/T116
        code_tui = stys['uri'].split('/')[-1]
        # Fetch the abbreviation of this TUI code
        sem_types.append(get_sem_type_abbr(code_tui, apikey))
    # Comma separated string
    sem_types = ",".join(sem_types)
    res['sem_types'] = sem_types
    return res


def get_sem_type_abbr(code_tui, apikey=None):
    """
    Function that fetches a semantic-type's abbreviation.
    Input:
        - code_tui: str,
        string of TUI code that will be looked up
        - apikey: str,
        UMLS REST services api-key. Default is None and
        the already establised service is used. Check get_umls_ticket
        function for details
    Output:
        string, abbreviation of the code (e.g. "gngm")
    """

    ticket = get_umls_ticket(apikey)
    url = "https://uts-ws.nlm.nih.gov/rest/semantic-network/current/TUI/" + code_tui
    r = requests.get(url, params={'ticket': ticket})
    r.encoding = 'utf-8'
    items = json.loads(r.text)
    jsonData = items["result"]
    return jsonData['abbreviation']