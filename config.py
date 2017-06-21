#!/usr/bin/env python

"""
Simple module wrapper to load settings file in order
to have it available in all modules.
"""
import yaml
import os
from Authentication import Authentication

settings_filename = os.path.join(os.path.dirname(__file__), 'settings.yaml')
with open(settings_filename, "r") as f:
    settings = yaml.load(f)

# API-kEY FOR UMLS REST TICKET SERVICES
umls_api = settings['apis']['umls']
# UMLS REST SERVICES INITIALIZATION OF CLIENT AND TICKET
# GRANTING SERVICE TO BE USED IN ALL CASES
AuthClient = Authentication(umls_api)
tgt = AuthClient.gettgt()

