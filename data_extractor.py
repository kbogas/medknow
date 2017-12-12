#!/usr/bin/python !/usr/bin/env python
# -*- coding: utf-8 -*


# Functions to extract knowledge from medical text. Everything related to 
# extraction needed for the knowledge base. Also, some wrappers for SemRep, 
# MetaMap and Reverb. Contains some enrichment routines for utilizing UTS
# services.


import json
import subprocess
import urllib2
import pymongo
import numpy as np
from nltk.tokenize import sent_tokenize
from config import settings
from pymetamap import MetaMap
from utilities import time_log, get_concept_from_cui, get_concept_from_source
from itertools import product
from multiprocessing import cpu_count, Pool
from unidecode import unidecode

def metamap_wrapper(text):
    """
    Function-wrapper for metamap binary. Extracts concepts
    found in text.

    !!!! REMEMBER TO START THE METAMAP TAGGER AND
        WordSense DISAMBIGUATION SERVER !!!!
    
    Input:
        - text: str,
        a piece of text or sentence
    Output:
       - a dictionary with key sents and values
       a list of the concepts found
    """

    # Tokenize into sentences
    sents = sent_tokenize(text)
    # Load Metamap Instance
    mm = MetaMap.get_instance(settings['load']['path']['metamap'])
    concepts, errors = mm.extract_concepts(sents, range(len(sents)))
    # Keep the sentence ids
    ids = np.array([int(concept[0]) for concept in concepts])
    sentences = []
    for i in xrange(len(sents)):
        tmp = {'sent_id': i+1, 'entities': [], 'relations': []}
        # Wanted concepts according to sentence
        wanted = np.where(ids == i)[0].tolist()
        for w_ind in wanted:
            w_conc = concepts[w_ind]
            if hasattr(w_conc, 'cui'):
                tmp_conc = {'label': w_conc.preferred_name, 'cui': w_conc.cui, 
                            'sem_types': w_conc.semtypes, 'score': w_conc.score}
                tmp['entities'].append(tmp_conc)
        sentences.append(tmp)
    if errors:
        time_log('Errors with extracting concepts!')
        time_log(errors)
    return {'sents': sentences, 'sent_text':text}


def runProcess(exe, working_dir):    
    """
    Function that opens a command line and runs a command.
    Captures the output and returns.
    Input:
        - exe: str,
        string of the command to be run. ! REMEMBER TO ESCAPE CHARS!
        - working_dir: str,
        directory where the cmd should be executed
    Output:
        - lines: list,
        list of strings generated from the command
    """

    p = subprocess.Popen(exe, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, cwd=working_dir, shell=True)
    lines = p.stdout.readlines()
    return lines


def stopw_removal(inp, stop):
    """
    Stopwords removal in line of text.
    Input:
        - inp: str,
        string of the text input
        - stop: list,
        list of stop-words to be removed 
    """

    # Final string to be returned
    final = ''
    for w in inp.lower().split():
        if w not in stop:
            final += w + ' '
    # Remove last whitespace that was added ' '
    final = final[:-1]
    return final


def create_text_batches(text, N=5000, buffer_ = 100):
    """
    Function that takes a long string and split it into
    batches of approximately length N. The actual length
    of each batch differs, as each batch end in the next
    dot found in the string after the N chars.
    Input:
        - text: str,
        piece of text to clean
        - N: int,
        split into strings of 5000 characters each
    Output:
        - chunks: list,
        list containing the string parts
    """
    M = len(text)
    chunks_num = M // N
    if M % N != 0:
        chunks_num += 1
    chunks = []
    end_ind = 0
    start_ind = 0
    i = 0
    while i < chunks_num:
        start_ind = end_ind
        prob_text = text[start_ind + N: start_ind + N + buffer_]
        if '.' in prob_text:
            end_ind = start_ind + N + prob_text.index('.')+1
        else:
            end_ind = start_ind + N
        chunks.append(text[start_ind:end_ind])
        i += 1
    chunks = [ch for ch in chunks if ch]
    return chunks



def reverb_wrapper(text, stop=None):
    """
    Function-wrapper for ReVerb binary. Extracts relations
    found in text.
    Input:
        - text: str,
        a piece of text or sentence
        - stop: list,
        list of stopwords to remove from the relations
    Output:
        - total: list,
        list of lists. Each inner list contains one relation in the form
        [subject, predicate, object]
    """
    total = []
    for sent in sent_tokenize(text):
        cmd = 'echo "' + sent + '"' "| ./reverb -q | tr '\t' '\n' | cat -n"
        reverb_dir = settings['load']['path']['reverb']
        result = runProcess(cmd, reverb_dir)
        # Extract relations from reverb output
        result = result[-3:]
        result = [row.split('\t')[1].strip('\n') for row in result]
        # Remove common stopwords from relations
        if stop:
            result = [stopw_removal(res, stop) for res in result]
        total.append(result)
    # Remove empty relations
    total = [t for t in total if t]
    return total




def cui_to_uri(api_key, cui):
    """
    Function to map from cui to uri if possible. Uses biontology portal
    Input:
        - api_key: str,
        api usage key change it in setting.yaml
        - cui: str,
        cui of the entity we wish to map the uri
    Output:
        - the uri found in string format or None
    """

    REST_URL = "http://data.bioontology.org"
    annotations = get_json_with_api(api_key, REST_URL + "/search?include_properties=true&q=" + urllib2.quote(cui))
    try:
        return annotations['collection'][0]['@id']
    except Exception, e:
        time_log(Exception)
        time_log(e)
        return None

def get_json_with_api(api_key, url):
    """
    Helper funtion to retrieve a json from a url through urlib2
    Input:
        - api_key: str,
        api usage key change it in setting.yaml
        - url: str,
        url to curl
    Output:
        - json-style dictionary with the curl results 
    """

    opener = urllib2.build_opener()
    opener.addheaders = [('Authorization', 'apikey token=' + api_key)]
    return json.loads(opener.open(url).read())


def threshold_concepts(concepts, hard_num=3, score=None):
    """
    Thresholding concepts from metamap to keep only the most probable ones.
    Currently supporting thresholding on the first-N (hard_num) or based on
    the concept score.
    Input:
        - concepts: list,
        list of Metamap Class concepts
        - hard_num: int,
        the first-N concepts to keep, if this thresholidng is selected
        - score: float,
        lowest accepted concept score, if this thresholidng is selected 
    """

    if hard_num:
        if hard_num >= len(concepts):
            return concepts
        elif hard_num < len(concepts):
            return concepts[:hard_num]
    elif score:
            return [c for c in concepts if c.score > score]
    else:
        return concepts
        



def get_name_concept(concept):
    """
    Get name from the metamap concept. Tries different variations and
    returns the name found.
    Input:
        - concept: Metamap class concept, as generated from mmap_extract
        for example
    Output:
        - name: str,
        the name found for this concept
    """

    name = ''
    if hasattr(concept, 'preferred_name'):
        name = concept.preferred_name
    elif hasattr(concept, 'long_form') and hasattr(concept, 'short_form'):
        name = concept.long_form + '|' + concept.short_form
    elif hasattr(concept, 'long_form'):
        name = concept.long_form
    elif hasattr(concept, 'short_form'):
        name =  concept.short_form
    else:
        name = 'NO NAME IN CONCEPT'
    return name



def metamap_ents(x):
    """
    Function to get entities in usable form.
    Exctracts metamap concepts first, thresholds them and
    tries to extract names and uris for the concepts to be
    more usable.
    Input:
        - x: str,
        sentence to extract entities
    Output:
        - ents: list,
        list of entities found. Each entity is a dictionary with
        fields id (no. found in sentence), name if retrieved, cui if 
        available and uri if found
    """

    # API KEY to biontology mapping from cui to uri
    API_KEY = settings['apis']['biont']
    concepts = mmap_extract(x)
    concepts = threshold_concepts(concepts)
    ents = []
    for i, concept in enumerate(concepts):
        ent = {}
        ent['ent_id'] = i
        ent['name'] = get_name_concept(concept)
        if hasattr(concept, 'cui'):
            ent['cui'] = concept.cui
            ent['uri'] = cui_to_uri(API_KEY, ent['cui']) 
        else:
            ent['cui'] = None
            ent['uri'] = None
        ents.append(ent)
    return ents


def extract_entities(text, json_={}):
    """
    Extract entities from a given text using metamap and
    generate a json, preserving infro regarding the sentence
    of each entity that was found. For the time being, we preserve
    both concepts and the entities related to them
    Input:
         - text: str,
        a piece of text or sentence
        - json_: dic,
        sometimes the json to be returned is given to us to be enriched
        Defaults to an empty json_
    Output:
        - json_: dic,
        json with fields text, sents, concepts and entities
        containg the final results
    """
    json_['text'] = text
    # Tokenize the text
    sents = sent_tokenize(text)
    json_['sents'] = [{'sent_id': i, 'sent_text': sent} for i, sent in enumerate(sents)]
    json_['concepts'], _ = mmap_extract(text)
    json_['entities'] = {}
    for i, sent in enumerate(json_['sents']):
        ents = metamap_ents(sent)
        json_['entities'][sent['sent_id']] = ents
    return json_

def extract_metamap(json_, key):
    """
    Task function to parse and extract concepts from json_ style dic, using
    the MetaMap binary.
    Input:
        - json_ : dic,
        json-style dictionary generated from the Parse object related
        to the specific type of input
        - key : str,
        string denoting the type of medical text to read from. Used to
        find the correct paragraph in the settings.yaml file.
    Output:
        - json_ : dic,
        the previous json-style dictionary enriched with medical concepts
    """
    # outerfield for the documents in json
    docfield = settings['out']['json']['itemfield']
    # textfield to read text from
    textfield = settings['out']['json']['json_text_field']
    N = len(json_[docfield])
    for i, doc in enumerate(json_[docfield]):
        text = clean_text(doc[textfield])
        if len(text) > 5000:
            chunks = create_text_batches(text)
            results = {'text': text, 'sents': []}
            sent_id = 0
            for chunk in chunks:
                tmp = metamap_wrapper(chunk)
                for sent in tmp['sents']:
                    sent['sent_id'] = sent_id
                    sent_id += 1
                    results['sents'].append(sent)
        else:
            results = metamap_wrapper(text)
        json_[docfield][i].update(results)
        proc = int(i/float(N)*100)
        if proc % 10 == 0 and proc > 0:
            time_log('We are at %d/%d documents -- %0.2f %%' % (i, N, proc))
    return json_


def enrich_with_triples(results, subject, pred='MENTIONED_IN'):
    """
    Enrich with rdf triples a json dictionary in the form of:
    entity-URI -- MENTIONED_IN -- 'Text 'Title'. Only entities with
    uri's are considered.
    Input:
        - results: dic,
        json-style dictionary genereated from the extract_entities function
        - subject: str,
        the name of the text document in which the entities are mentioned
        - pred: str,
        the predicate to be used as a link between the uri and the title
    Output:
        - results: dic,
        the same dictionary with one more 
    """
    triples = []
    for sent_key, ents in results['entities'].iteritems():
        for ent in ents:
            if ent['uri']:
               triples.append({'subj': ent['uri'], 'pred': pred, 'obj': subject})
    results['triples'] = triples
    return results
        
def force_to_unicode(text):
    "If text is unicode, it is returned as is. If it's str, convert it to Unicode using UTF-8 encoding"
    return text if isinstance(text, unicode) else text.decode('utf8', 'ignore')


def toAscii_wrapper(text):
    """
    Function wrapper for Lexical Tool toAscii:
    https://lexsrv3.nlm.nih.gov/LexSysGroup/Projects/lvg/current/docs/userDoc/tools/toAscii.html
    Converts input to ascii ready for SemRep
    Input:
        - text: str,
        a piece of text or sentence'
    Output:
        - text: str,
        the same text with changes
    """
    text = clean_text(text)
    #text = repr(text)
    cmd = 'echo "' + text + '" | ./toAscii'
    toAscii_dir = settings['load']['path']['toAscii']
    lines = runProcess(cmd, toAscii_dir)
    return lines[0]

def semrep_wrapper(text):
    """
    Function wrapper for SemRep binary. It is called with flags
    -F only and changing this will cause this parsing to fail, cause
    the resulting lines won't have the same structure.
    Input:
        - text: str,
        a piece of text or sentence
    Output:
        - results: dic,
        jston-style dictionary with fields text and sents. Each
        sentence has entities and relations found in it. Each entity and
        each relation has attributes denoted in the corresponding
        mappings dictionary. 
    """
    # Exec the binary
    # THIS SHOULD FIX ENCODING PROBLEMS???
    text = clean_text(text)
    utf8 = force_to_unicode(text)
    text = unidecode(utf8)
    # text = toAscii_wrapper(text)
    # THIS IS NEEDED FOR ANY ARTIFACTS!
    
    text = repr(text)
    cmd = "echo " + text + " | ./semrep.v1.7 -L 2015 -Z 2015AA -F"
    #print cmd
    semrep_dir = settings['load']['path']['semrep']
    lines = runProcess(cmd, semrep_dir)
    # mapping of line elements to fields
    mappings = {
        "text": {
            "sent_id": 4,
            "sent_text": 6
        },
        "entity": {
            'cuid': 6,
            'label': 7,
            'sem_types': 8,
            'score': 15
        },
        "relation": {
            'subject__cui': 8,
            'subject__label': 9,
            'subject__sem_types': 10,
            'subject__sem_type': 11,
            'subject__score': 18,
            'predicate__type': 21,
            'predicate': 22,
            'negation': 23,
            'object__cui': 28,
            'object__label': 29,
            'object__sem_types': 30,
            'object__sem_type': 31,
            'object__score': 38,
        }
    }
    results = {'sents': [], 'text': text}
    for line in lines:
        # If Sentence
        if line.startswith('SE'):
            ##### DEPRECATED AS IN CLEAN TEXT WE REMOVE TABS FROM TEXT #######
            # Temporary workaround to remove read |-delimited semrep output
            # Without mixing up tabs contained in the text
            # line = line.replace('\|', '!@#$')
            # elements = line.split('|')
            # elements = [el.replace('!@#$', '\|') for el in elements]
            #########################  DEPRECATED ###########################
            elements = line.split('|')
            # New sentence that was processed
            if elements[5] == 'text':
                tmp = {"entities": [], "relations": []}
                for key, ind in mappings['text'].iteritems():
                    tmp[key] = elements[ind]
                results['sents'].append(tmp)
            # A line containing entity info
            if elements[5] == 'entity':
                tmp = {}
                for key, ind in mappings['entity'].iteritems():
                    if key == 'sem_types':
                        tmp[key] = elements[ind].split(',')
                    tmp[key] = elements[ind]
                results['sents'][-1]['entities'].append(tmp)
            # A line containing relation info
            if elements[5] == 'relation':
                tmp = {}
                for key, ind in mappings['relation'].iteritems():
                    if 'sem_types' in key:
                        tmp[key] = elements[ind].split(',')
                    else:
                        tmp[key] = elements[ind]
                results['sents'][-1]['relations'].append(tmp)
    return results


def clean_text(text):
    """
    Escape specific characters for command line call of SemRep. This
    could be updated in the future to more sophisticated transformations.
    Input:
        - text: str,
        piece of text to clean
    Output:
        - text: str,
        the same text with cmd escaped parenthesis and removing '
    """
    replace_chars = [('(', ' '), (')', ' '), ("'",  ' '), ('\n', " "), ('\t', ' '), (';', " "), 
                     ("}", " "), ("{", " "), ("|", " "), ("&", " "), ("/", ' ')]
    for unw_pair in replace_chars:
        text = text.replace(unw_pair[0], unw_pair[1])
    text = ' '.join(text.split())
    return text


def extract_semrep(json_, key):
    """
    Task function to parse and extract concepts from json_ style dic, using
    the SemRep binary.
    Input:
        - json_ : dic,
        json-style dictionary generated from the Parse object related
        to the specific type of input
        - key : str,
        string denoting the type of medical text to read from. Used to
        find the correct paragraph in the settings.yaml file.
    Output:
        - json_ : dic,
        the previous json-style dictionary enriched with medical concepts
    """
    # outerfield for the documents in json
    if key == 'mongo':
        key = 'json'
    docfield = settings['out']['json']['itemfield']
    # textfield to read text from
    textfield = settings['out']['json']['json_text_field']
    N = len(json_[docfield])
    for i, doc in enumerate(json_[docfield]):
        print doc['id']
        text = doc[textfield]
        if len(text) > 5000:
            chunks = create_text_batches(text)
            results = {'text': text, 'sents': []}
            sent_id = 0
            # c = 0
            for chunk in chunks:
                # c += 1
                # print 'CHUNK %d' % c 
                # print chunk
                # print '~'*50
                tmp = semrep_wrapper(chunk)
                for sent in tmp['sents']:
                    sent['sent_id'] = sent_id
                    sent_id += 1
                    results['sents'].append(sent)
        else:
            results = semrep_wrapper(text)
        json_[docfield][i].update(results)
        proc = int(i/float(N)*100)
        if proc % 10 == 0 and proc > 0:
            time_log('We are at %d/%d documents -- %0.2f %%' % (i, N, proc))
    return json_




def extract_semrep_parallel(json_, key):
    """
    Task function to parse and extract concepts from json_ style dic, using
    the SemRep binary. It uses multiprocessing for efficiency.
    Input:
        - json_ : dic,
        json-style dictionary generated from the Parse object related
        to the specific type of input
        - key : str,
        string denoting the type of medical text to read from. Used to
        find the correct paragraph in the settings.yaml file.
    Output:
        - json_ : dic,
        the previous json-style dictionary enriched with medical concepts
    """
    # outerfield for the documents in json
    docfield = settings['out']['json']['itemfield']
    N = len(json_[docfield])
    try:
        N_THREADS = int(settings['num_cores'])
    except:
        N_THREADS = cpu_count()
    batches = chunk_document_collection(json_[docfield], N_THREADS)
    len_col = " | ".join([str(len(b)) for b in batches])
    time_log('Will break the collection into batches of: %s documents!' % len_col)
    batches = [{docfield: batch} for batch in batches]
    data = zip(batches, [key for batch in batches])
    pool = Pool(N_THREADS, maxtasksperchild=1)
    res = pool.map(semrep_parallel_worker, data)
    pool.close()
    pool.join()
    del pool
    tmp = {docfield: []}
    for batch_res in res:
        tmp[docfield].extend(batch_res[docfield])
    for i, sub_doc in enumerate(json_[docfield]):
        for sub_doc_new in tmp[docfield]:
            if sub_doc_new['id'] == sub_doc['id']:
                json_[docfield][i].update(sub_doc_new)
                break
    time_log('Completed multiprocessing extraction!')
    return json_


def chunk_document_collection(seq, num):
    """
    Helper function to break a collection of N = len(seq) documents
    to num batches.
    Input:
        - seq: list,
        a list of documents
        - num: int,
        number of batches to be broken into. This will usually be
        equal to the number of cores available
    Output:
        - out: list,
        a list of lists. Each sublist contains the batch-collection
        of documents to be used.
    """
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out


def semrep_parallel_worker((json_, key)):
    """
    Just a worker interface for the different SemRep
    executions.
    Input:
        - json_ : dic,
        json-style dictionary generated from the Parse object related
        to the specific type of input
        - key : str,
        string denoting the type of medical text to read from. Used to
        find the correct paragraph in the settings.yaml file.
    Output:
        - res : dic,
        the previous json-style dictionary enriched with medical concepts

    """
    res = extract_semrep(json_, key)
    return res



def get_concepts_from_edges_parallel(json_, key):
    """
    Same work as the get_concepts_from_edges_paralle. It uses multiprocessing 
    for efficiency.
    Input:
        - json: dict,
        json-style dictionary with a field containing
        relations
        - key : str,
        string denoting the type of medical text to read from. Used to
        find the correct paragraph in the settings.yaml file.
    Output:
        - json: dict,
        the updated json-style dictionary where the relations
        in the list have been updated and each subject-object has been
        mapped to the according

    """
    outfield = settings['load'][key]['itemfield']
    N = len(json_[outfield])
    try:
        N_THREADS = int(settings['num_cores'])
    except:
        N_THREADS = cpu_count()
    batches = chunk_document_collection(json_[outfield], N_THREADS)
    len_col = " | ".join([str(len(b)) for b in batches])
    time_log('Will break the edges into batches of: %s documents!' % len_col)
    batches = [{outfield: batch} for batch in batches]
    data = zip(batches, [key for batch in batches])
    pool = Pool(N_THREADS, maxtasksperchild=1)
    res = pool.map(edges_parallel_worker, data)
    pool.close()
    pool.join()
    del pool
    json_ = {outfield: []}
    for batch_res in res:
        json_[outfield].extend(batch_res[outfield])
    time_log('Completed multiprocessing extraction!')
    return json_




def edges_parallel_worker((json_, key)):
    """
    Just a worker interface for the parallel enrichment
    executions.
    Input:
        - json_ : dic,
        json-style dictionary generated from the Parse object related
        to the specific type of input
        - key : str,
        string denoting the type of medical text to read from. Used to
        find the correct paragraph in the settings.yaml file.
    Output:
        - res : dic,
        expected outcome of get_concepts_from_edges

    """
    res = get_concepts_from_edges(json_, key)
    return res


def get_concepts_from_edges(json_, key):
    """
    Get concept-specific info related to an entity from a list
    containing relations. Each subject-object in the relations
    list is expressed in a another data source(MESH, DRUGBANK etc)
    and their unique identifier is provided. Also, articles and new
    kinde of sub-obj are handled.
    Input:
        - json: dict,
        json-style dictionary with a field containing
        relations
        - key : str,
        string denoting the type of medical text to read from. Used to
        find the correct paragraph in the settings.yaml file.
    Output:
        - json: dict,
        the updated json-style dictionary where the relations
        in the list have been updated and each subject-object has been
        mapped to the according

    """

    # docfield containing list of elements containing the relations
    outfield = settings['load'][key]['itemfield']
    # field containing the type of the node for the subject
    sub_type = settings['load'][key]['sub_type']
    # field containing the source of the node for the subject
    sub_source = settings['load'][key]['sub_source']
    # field containing the type of the node for the object
    obj_type = settings['load'][key]['obj_type']
    # field containing the source of the node for the object
    obj_source = settings['load'][key]['obj_source']
    new_relations = []
    uri = settings['load']['mongo']['uri']
    db_name = settings['load']['mongo']['db']
    collection_name = settings['load']['mongo']['cache_collection']
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    cur = collection.find({})
    cache = {}
    for item in cur:
        cache[item['key']] = item['value']
    N = len(json_[outfield])
    for ii, triple in enumerate(json_[outfield]):
        print triple
        try:
            if sub_source == 'UMLS':
                if not(triple['s'] in cache):
                    ent = get_concept_from_cui(triple['s'])
                    cache[triple['s']] = ent
                    collection.insert_one({'key':triple['s'],'value':ent})
                    print 'INSERTED in UMLS %s' % triple['s']
                else:
                    ent = cache[triple['s']]
                if (type(ent['sem_types']) == list and len(ent['sem_types']) > 1):
                    sem_types = ';'.join(ent['sem_types'])
                elif (',' in ent['sem_types']):
                    sem_types = ';'.join(ent['sem_types'].split(','))
                else:
                    sem_types = ent['sem_types']

                triple_subj = [{'id:ID': ent['cuid'], 
                                'label': ent['label'], 
                                'sem_types:string[]': sem_types}]
            elif (sub_source == 'PMC') or (sub_source == 'TEXT') or (sub_source == 'None'):
                triple_subj = [{'id:ID': triple['s']}]
            else:
                if not(triple['s'] in cache):
                    ents = get_concept_from_source(triple['s'], sub_source)
                    cache[triple['s']] = ents
                    collection.insert_one({'key':triple['s'],'value':ents})
                    print 'INSERTED in other %s' % triple['s']
                else:
                    ents = cache[triple['s']]
                triple_subj = []
                for ent in ents:
                    if (type(ent['sem_types']) == list and len(ent['sem_types']) > 1):
                        sem_types = ';'.join(ent['sem_types'])
                    elif (',' in ent['sem_types']):
                        sem_types = ';'.join(ent['sem_types'].split(','))
                    else:
                        sem_types = ent['sem_types']

                    triple_subj.append({'id:ID': ent['cuid'], 
                                    'label': ent['label'], 
                                    'sem_types:string[]': sem_types})
            if obj_source == 'UMLS':
                if not(triple['o'] in cache):
                    ent = get_concept_from_cui(triple['o'])
                    cache[triple['o']] = ent
                    collection.insert_one({'key':triple['o'],'value':ent})
                    print 'INSERTED in UMLS %s' % triple['o']
                else:
                    ent = cache[triple['o']]
                if (type(ent['sem_types']) == list and len(ent['sem_types']) > 1):
                    sem_types = ';'.join(ent['sem_types'])
                elif (',' in ent['sem_types']):
                    sem_types = ';'.join(ent['sem_types'].split(','))
                else:
                    sem_types = ent['sem_types']
                triple_obj = [{'id:ID': ent['cuid'], 
                                'label': ent['label'], 
                                'sem_types:string[]': sem_types}]
            elif (obj_source == 'PMC') or (obj_source == 'TEXT') or (obj_source == 'None'):
                triple_obj = [{'id:ID': triple['o']}]
            else:
                if not(triple['o'] in cache):
                    ents = get_concept_from_source(triple['o'], obj_source)
                    cache[triple['o']] = ents
                    collection.insert_one({'key':triple['o'],'value':ents})
                    print 'INSERTED in other %s' % triple['o']
                else:
                    ents = cache[triple['o']]
                triple_obj = []
                for ent in ents:
                    if (type(ent['sem_types']) == list and len(ent['sem_types']) > 1):
                        sem_types = ';'.join(ent['sem_types'])
                    elif (',' in ent['sem_types']):
                        sem_types = ';'.join(ent['sem_types'].split(','))
                    else:
                        sem_types = ent['sem_types']

                    triple_obj.append({'id:ID': ent['cuid'], 
                                    'label': ent['label'], 
                                    'sem_types:string[]': sem_types})
            combs = product(triple_subj, triple_obj)
            for comb in combs:
                new_relations.append({'s':comb[0], 'p':triple['p'], 'o':comb[1]})
        except Exception, e:
            time_log(e)
            time_log('S: %s | P: %s | O: %s' % (triple['s'],triple['p'],triple['o']))
            time_log('Skipped the above edge! Probably due to concept-fetching errors!')
        proc = int(ii/float(N)*100)
        if proc % 10 == 0 and proc > 0:
            time_log('We are at %d/%d edges transformed -- %0.2f %%' % (ii, N, proc))
        # if ii % 100 == 0 and ii > 9:
        #     time_log("Edges Transformation Process: %d -- %0.2f %%" % (ii, 100*ii/float(len(json_[outfield]))))
    json_[outfield] = new_relations
    return json_