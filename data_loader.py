#!/usr/bin/python !/usr/bin/env python
# -*- coding: utf-8 -*


# Functions to extract knowledge from medical text. Everything related to 
# reading and parsing.

import json
import py2neo
import pymongo
import langid
import pandas as pd
from config import settings
from utilities import time_log
from multiprocessing import cpu_count
import ijson.backends.yajl2_cffi as ijson2


def load_mongo(key):
    """
    Parse collection from mongo
    Input:
        - key: str,
        the type of input to read
    Output:
        - json_ : dic,
        json-style dictionary with a field containing
        documents
    """

    # input mongo variables from settings.yaml
    uri = settings['load']['mongo']['uri']
    db_name = settings['load']['mongo']['db']
    collection_name = settings['load']['mongo']['collection']
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    # itemfield containing list of elements
    out_outfield = settings['out']['json']['itemfield']
    json_ = {out_outfield: []}
    cur = collection.find({})
    for item in cur:
        del item['_id']
        json_[out_outfield].append(item)
    return json_


def load_mongo_batches(key, N_collection, ind_=0):
    """
    Parse collection from mongo to be processed in streaming/parallel fashion.
    Fetches step = (N X numb_cores) of documents starting from ind_ and
    delivers it to the rest of the pipeline.
    Input:
        - key: str,
        the type of input to read
        - N_collection: int,
        total collection length
        - ind: int,
        the starting point of the batch (or stream) to be read
    Output:
        - json_ : dic,
        json-style dictionary with a field containing
        items
    """
    # input file path from settings.yaml
    uri = settings['load']['mongo']['uri']
    db_name = settings['load']['mongo']['db']
    collection_name = settings['load']['mongo']['collection']
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    # itemfield containing list of elements
    out_outfield = settings['out']['json']['itemfield']
    json_ = {out_outfield: []}
    stream_flag = str(settings['pipeline']['in']['stream']) == 'True'
    # batch size in case of streaming enviroment is just one
    if stream_flag:
        step = 1
    # else N_THREADS*
    else:
        try:
            N_THREADS = int(settings['num_cores'])
        except:
            N_THREADS = cpu_count()
        try:
            batch_per_core = int(settings['batch_per_core'])
        except:
            batch_per_core = 100
        step = N_THREADS * batch_per_core
    print ind_, step
    time_log("Will start from %d/%d and read %d items" % (ind_, N_collection, step))
    if step > N_collection:
        step = N_collection
    else:
        cur = collection.find({}, skip=ind_, limit=step)
        c = 0
        for item in cur:
            del item['_id']
            c += 1
            json_[out_outfield].append(item)
        return json_, ind_ + step

def load_file(key):
    """
    Parse file containing items.
    Input:
        - key: str,
        the type of input to read
    Output:
        - json_ : dic,
        json-style dictionary with items
    """

    # input file path from settings.yamml
    if key == 'med_rec':
        json_ = parse_medical_rec()
    else:
        inp_path = settings['load']['path']['file_path']
        with open(inp_path, 'r') as f:
            json_ = json.load(f, encoding='utf-8')
    return json_


def load_file_batches(key, N_collection, ind_=0):
    """
    Parse collection from file to be processed in streaming/parallel fashion.
    Fetches step = (N X numb_cores) of documents starting from ind_ and
    delivers it to the rest of the pipeline.
    Input:
        - key: str,
        the type of input to read
        - N_collection: int,
        total collection length
        - ind: int,
        the starting point of the batch (or stream) to be read  
    Output:
        - json_ : dic,
        json-style dictionary with a field containing
        items
    """
    # Filepath to item collection
    inp_path = settings['load']['path']['file_path']
    # Document iterator field in the collection
    infield = settings['load'][key]['itemfield']
    # itemfield containing list of elements
    out_outfield = settings['out']['json']['itemfield']
    # The generated json_
    json_ = {out_outfield: []}
    # Check if streaming
    stream_flag = str(settings['pipeline']['in']['stream']) == 'True'
    # batch size in case of streaming enviroment is just one
    if stream_flag:
        step = 1
    # else N_THREADS* Batches_per_core
    else:
        try:
            N_THREADS = int(settings['num_cores'])
        except:
            N_THREADS = cpu_count()
        try:
            batch_per_core = int(settings['batch_per_core'])
        except:
            batch_per_core = 100
        step = N_THREADS * batch_per_core
    if step > N_collection:
        step = N_collection
    # Collection counter
    col_counter = 0
    #print infield
    time_log("Will start from %d/%d and read %d items" % (ind_, N_collection, step))
    with open(inp_path, 'r') as f:
        docs = ijson2.items(f, '%s.item' % infield)
        for c, item in enumerate(docs):
            if c < ind_:
                continue
            json_[out_outfield].append(item)
            #print json_
            col_counter += 1
            if col_counter >= step:
                break
    
    if col_counter == 0:
        #print 'Col_counter'
        #print col_counter
        return None, None
    else:
        #print json_
        return json_, ind_ + step


def parse_medical_rec():
    """
    Parse file containing medical records.
    Output:
        - json_ : dic,
        json-style dictionary with documents containing
        a list of dicts, containing the medical record and the corresponding
        attributes
    """

    # path to file to read from
    inp_path = settings['load']['path']['file_path']
    # csv seperator from settings.yaml
    sep = settings['load']['med_rec']['sep']
    # textfield to read text from
    textfield = settings['load']['med_rec']['textfield']
    # idfield where id of document is stored
    idfield = settings['load']['med_rec']['idfield']
    with open(inp_path, 'r') as f:
        diag = pd.DataFrame.from_csv(f, sep='\t')
    # Get texts
    texts = diag[textfield].values
    # outerfield for the documents in json
    itemfield = settings['out']['json']['itemfield']
    # textfield to read text from
    out_textfield = settings['out']['json']['json_text_field']
    # labelfield where title of the document is stored
    out_labelfield = settings['out']['json']['json_label_field']
    diag[out_labelfield] = ['Medical Record' + str(i) for i in diag.index.values.tolist()]
    if not('journal' in diag.columns.tolist()):
        diag['journal'] = ['None' for i in diag.index.values.tolist()]
    # Replace textfiled with out_textfield
    diag[out_textfield] = diag[textfield]
    del diag[textfield]
    # Replace id with default out_idfield
    diag['id'] = diag[idfield]
    del diag[idfield]
    json_ = {itemfield: diag.to_dict(orient='records')}
    return json_




def parse_text(json_):
    """
    Helper function to parse the loaded documents. Specifically,
    we ignore documents with no assigned text field. We also provide
    an empty string for label if non-existent. Other than that, norma-
    lizing the id,text and label fields as indicated in the settings.
    Input:
        - json_: dicm
        json-style dictionary with a field containing
        items
    Output:
        - json_ : dic,
        json-style dictionary with a field containing normalized and
        cleaned items
    """

    ## Values to read from

    # itemfield containing list of elements containing text
    outfield = settings['load']['text']['itemfield']
    # textfield to read text from
    textfield = settings['load']['text']['textfield']
    # idfield where id of document is stored
    idfield = settings['load']['text']['idfield']
    # labelfield where title of the document is stored
    labelfield = settings['load']['text']['labelfield']
    
    ## Values to replace them with ##

    # itemfield containing list of elements
    out_outfield = settings['out']['json']['itemfield']
    # textfield to read text from
    out_textfield = settings['out']['json']['json_text_field']
    # idfield where id of document is stored
    out_idfield = settings['out']['json']['json_id_field']
    # labelfield where title of the document is stored
    out_labelfield = settings['out']['json']['json_label_field']
    json_[outfield] = [art for art in json_[outfield] if textfield in art.keys()]
    json_[outfield] = [art for art in json_[outfield] if langid.classify(art[textfield])[0] == 'en']
    for article in json_[outfield]:
        article[out_textfield] = article.pop(textfield)
        article[out_idfield] = article.pop(idfield)
        if labelfield != 'None':
            article[out_labelfield] = article.pop(labelfield)
        else:
            article[out_labelfield] = ' '
        if not('journal' in article.keys()):
            article['journal'] = 'None'
    json_[out_outfield] = json_.pop(outfield)
    # N = len(json_[out_outfield])
    # json_[out_outfield] = json_[out_outfield][(2*N/5):(3*N/5)]
    json_[out_outfield] = json_[out_outfield][:]
    return json_


def parse_remove_edges(key=None):
    """
    Dummy function to conform with the pipeline when
    we just want to delete edges instead of inserting
    them.
    Output:
        - an empty dic to be passed around, as to 
        conform to the pipeline schema 
    """

    # Read neo4j essentials before 
    host = settings['neo4j']['host']
    port = settings['neo4j']['port']
    user = settings['neo4j']['user']
    password = settings['neo4j']['password']
    try:
        graph = py2neo.Graph(host=host, port=port, user=user, password=password)
    except Exception, e:
        #time_log(e)
        #time_log("Couldn't connect to db! Check settings!")
        exit(2)
    quer1 = """ MATCH ()-[r]->() WHERE r.resource = "%s" DELETE r;""" % (settings['neo4j']['resource'])
    f = graph.run(quer1)
    rem = f.stats()['relationships_deleted']
    quer2 = """ MATCH ()-[r]->() WHERE "%s" in  r.resource SET 
    r.resource = FILTER(x IN r.resource WHERE x <> "%s");""" % (settings['neo4j']['resource'], settings['neo4j']['resource'])
    f = graph.run(quer2)
    alt = f.stats()['properties_set']
    time_log('Removed %d edges that were found only in %s' % (rem, settings['neo4j']['resource']))
    time_log("Altered %s edges' resource attribute associated with %s" % (alt, settings['neo4j']['resource']))
    exit(1)
    return {}


def get_collection_count(source, type):
    """
    Helper function to get total collection length.
    Input:
        - source: str, value denoting where we will read from (e.g 'mongo')
        - type: str, value denoting what we will read (e.g. text, edges)
    Output:
        - N_collection: int,
        number of items in the collection
    """
    if source == 'file':
        inp_path = settings['load']['path']['file_path'] 
        # Document iterator field in the collection
        infield = settings['load'][type]['itemfield']
        with open(inp_path, 'r') as f:
            docs = ijson2.items(f, '%s.item' % infield)
            N_collection = 0
            for item in docs:
                N_collection += 1
    elif source == 'mongo':
        # input mongo variables from settings.yaml
        uri = settings['load']['mongo']['uri']
        db_name = settings['load']['mongo']['db']
        collection_name = settings['load']['mongo']['collection']
        client = pymongo.MongoClient(uri)
        db = client[db_name]
        collection = db[collection_name]
        N_collection = collection.count()
    else:
        time_log("Can't calculate total collection count for source type %s" % settings['in']['source'])
        raise NotImplementedError
    return N_collection
