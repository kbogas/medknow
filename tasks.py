#!/usr/bin/python !/usr/bin/env python
# -*- coding: utf-8 -*


# Functions tha combine modular subfunctions creating
# a task to complete, such as reading from file, extracting concepts
# and saving to disk again.

from config import settings
from utilities import time_log
from data_loader import parse_medical_rec, parse_json, extract_semrep
from data_saver import save_csv, save_neo4j, save_json, create_neo4j_results, create_neo4j_csv


class Parser(object):
    """
    Parser class for reading input. According to which pipeline
    task it is called upon, it parses the appropriate file.
    Filepaths and details according to settings.yaml.
    """

    def __init__(self, key, name=None):
        """
        Initialization of the class. Currently keys are:
        ['med_rec', 'json']. The name is only for pretty-printing
        purposes.
        """

        self.key = key
        if self.key == 'med_rec':
            self.func = parse_medical_rec
        elif self.key == 'json':
            self.func = parse_json
        if name:
            self.name = name
        else:
            self.name = self.key

    def read(self):
        """
        Run the corresponding parsing function and return the .json_
        dictionary result.
        """

        json_ = self.func()
        time_log('Completed Parsing. Read: %d documents!' % len(json_[settings['out']['json']['json_doc_field']]))
        return json_


class Extractor(object):
    """
    Class for extracting concepts/entities and relations from medical text.
    Expects to work with json files generated from the corresponding Parser
    objects. Currently ['semrep'] implemented.
    Filepaths and details according to settings.yaml.
    """

    def __init__(self, key, parser_key, name=None):
        """
        Initialization of the class.
        Input:
            - key: str,
            string denoting what extraction task is to take place
            - parser_key: str,
            string denoting what type of input to expect
            - name: str,
            optional string for the tast to be printed
        """

        self.key = key
        self.parser_key = parser_key
        if self.key == 'semrep':
            self.func = extract_semrep
        elif self.key == 'metamap':
            raise NotImplementedError
            # self.func = extract_metamap
        elif self.key == 'reverb':
            raise NotImplementedError
            # self.func = extract_reverb
        if name:
            self.name = name
        else:
            self.name = self.key

    def run(self, json):
        """
        Run the corresponding extracting function and return the .json_
        dictionary result.
        """

        if type(json) == dict:
            json_ = self.func(json, self.parser_key)
            time_log('Completed extracting using %s!' % self.name)
        else:
            print 'Unsupported type of json to work on!'
            print 'Task : %s  --- Type of json: %s' % (self.name, type(json))
            print json
            json_ = {}
        return json_


class Dumper(object):
    """
    Class for saving the extracted results. Expects to work with json files
    generated from the previous extraction phases. Currently implemented
    dumping methods for keys:
        -json : for the enriched medical documents
        -csv : for nodes, relations before importing into neo4j
        -neo4j: for nodes, relations updating neo4j db directly
    Filepaths and details according to settings.yaml.
    Params:
        - key: str,
        one of the json, csv, neo4j
        - inp_key: str,
        the Parser key for this pipeline
        - name: str,
        Name of the Dumper. For printing purposes only
    """

    def __init__(self, key, inp_key='json', name=None):
        self.key = key
        if self.key == 'json':
            self.transform = None
            self.func = save_json
        elif self.key == 'csv' or self.key == 'neo4j':
            self.transform = create_neo4j_results
            self.func = create_neo4j_csv
        elif self.key == 'neo4j':
            self.transform = create_neo4j_results
            self.func = updat
        if inp_key == 'med_rec' or inp_key == 'json':
            self.type_ = 'harvester'
        elif inp_key == 'edges':
            self.type_ = 'edges'
        if name:
            self.name = name
        else:
            self.name = self.key

    def save(self, json_):
        if type(json_) == dict:
            if self.transform:
                results = self.transform(json_, self.type_)
            else:
                results = json_ 
            json_ = self.func(results)
            time_log('Completed saving data. Results saved in:\n %s' % settings['out'][self.key]['out_path'])
        else:
            print 'Unsupported type of json to work on!'
            print 'Task : %s  --- Type of json: %s' % (self.name, type(json))
            print json
            json_ = {}
        return json_


class taskCoordinator(object):
    """
    Orchestrator class for the different saving values.
    """

    def __init__(self):
        self.pipeline = {}
        self.phases = ['in', 'trans', 'out']
        for phase, dic_ in sorted(settings['pipeline'].iteritems()):
            self.pipeline[phase] = {}
            for key, value in dic_.iteritems():
                if value:
                    self.pipeline[phase][key] = value

    def run(self):
        for phase in self.phases:
            dic = self.pipeline[phase]
            if phase == 'in':
                parser = Parser(self.pipeline[phase]['inp'])
                json_ = parser.read()
            if phase == 'trans':
                for key, value in dic.iteritems():
                    if value:
                        extractor = Extractor(key, parser.key)
                        json_ = extractor.run(json_)
                        print '### EXTRACTOR ###'*50
                        print json_
            if phase == 'out':
                for key, value in dic.iteritems():
                    if value:
                        print '$$$$ DUMPER $$$'*50
                        key, value
                        dumper = Dumper(key, parser.key)
                        dumper.save(json_)

    def print_pipeline(self):
        print('#'*30 + ' Pipeline Schedule' + '#'*30)
        for phase in self.phases:
            dic = self.pipeline[phase]
            if phase == 'in':
                print('Will read from: %s' % settings['load'][dic['inp']]['inp_path'])
            if phase == 'trans':
                print('Will use the following transformation utilities:')
                for key, value in dic.iteritems():
                    print ('- %s' % key)
            if phase == 'out':
                print('Will save the outcome as follows:')
                for key, value in dic.iteritems():
                    print('%s  : %s' % (key, settings['out'][key]['out_path']))
        print('#'*30 + ' Pipeline Schedule ' + '#'*30)