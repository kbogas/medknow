#!/usr/bin/python !/usr/bin/env python
# -*- coding: utf-8 -*


# Functions tha combine modular subfunctions creating
# a task to complete, such as reading from file, extracting concepts
# and saving to disk again.

import json
import os
import py2neo
import csv
import subprocess
import urllib2
import requests
import unicodecsv as csv2
import pandas as pd
from nltk.tokenize import sent_tokenize
from config import settings
from utilities import time_log
from data_loader import semrep_wrapper



def extract_medical_rec(json_):
    """
    Task function to parse and extract concepts from medical records.
    """

    for med_rec in json_['medical_records']:
        results = semrep_wrapper(clean_text(med_rec['text']))
        # Update the medical records
        json_['medical_records'][-1] = results
    return json_


def parse_medical_rec():
    """
    Parse file containing medical records.
    Output:
        - json_ : dic,
        json-style dictionary with field medical_records containing
        a list of dicts, with field text, containing the medical record
        json_ = {'medical_records': [{'text':...}, {'text':...}]}
    """

    # input file path from settings.yaml
    inp_path = settings['load']['med_rec']['inp_path']
    # csv seperator from settings.yaml
    sep = settings['load']['med_rec']['sep']
    # textfield to read text from
    textfield = settings['load']['med_rec']['text_field']
    with open(inp_path, 'r') as f:
        diag = pd.DataFrame.from_csv(f, sep=sep)
    # Get texts
    texts = diag[textfield].values
    json_ = {'medical_records': []}
    for text in texts:
        json_['medical_records'].append({'text': text})
    return json_


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

    text = text.replace('(', '\(').replace(')', '\)').replace("'",  ' ')
    return text

def extract_from_json(infile, semrep_path):
    with open(infile, 'r') as f:
        json_ = json.load(f, encoding='utf-8')
    for i, doc in enumerate(json_['documents']):
        print i
        text = clean_text(doc['abstractText'])
        results = semrep_wrapper(text, semrep_path)
        doc['sents'] = results['sents']
        json_['documents'][i] = doc
    return json_
        #total_res['medical_records'].append(results)
    #return total_res



class Parser(object):
    """
    Parser class for reading input. According to which pipeline
    task it is called upon, it parses the appropriate file.
    """

    def __init__(self, key):
        self.key = key
        if self.key == 'med_rec':
            self.func = parse_medical_rec
        elif self.key == 'json':
            self.func = parse_json

    def read(self):
        json_ = self.func()
        return json_


class Extractor(object):
    """
    Class for each task to be completed. Mainly implements the
    run fuction. Expects to work with json files.
    """

    def __init__(self, key, name=None):
        self.key = key
        if self.key == 'med_rec':
            self.func = extract_medical_rec
        elif self.key == 'json':
            self.func = extract_json
        if not(self.name):
            self.name = key

    def run(json):
        if type(json) == dict:
            json_ = self.run(json)
        elif type(json) == str or not(json):
            json = Parser(self.key).read()
            json_ = self.run(json)
        else:
            print 'Unsupported type of json to work on!'
            print 'Task : %s  --- Type of json: %s' % (self.name, type(json))
            print json
            json_ = {}
        return json_


class taskCoordinator(object):

    def __init__(self):
        self.pipeline = {}
        self.phases = ['in', 'trans', 'out']
        for phase, dic_ in sorted(settings['pipeline'].iteritems()):
            self.pipeline[phase] = {}
            for key, value in dic_.iteritems():
                if value:
                    self.pipeline[phase][key] = value


    def print_pipeline(self):
        print('#'*30 + ' Pipeline Schedule' + '#'*30)
        for phase in self.phases:
            dic = self.pipeline[phase]
            if phase == 'in':
                print('Will read from: %s' % settings['load']['vars'][dic['inp']]['inp_path'])
            if phase == 'trans':
                print('Will use the following transformation utilities:')
                for key, value in dic.iteritems():
                    print ('- %s' % key)
            if phase == 'out':
                print('Will save the outcome as follows:')
                for key, value in dic.iteritems():
                    print('%s  : %s' % (key, settings['out'][key]['out_path']))
        print('#'*30 + ' Pipeline Schedule' + '#'*30)