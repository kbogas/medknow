#!/usr/bin/python !/usr/bin/env python
# -*- coding: utf-8 -*

# !!!!!!!!!  FIRST CONFIGURE SETTINGS.YAML TO MATCH YOUR NEEDS !!!!!!!!!!
# Simple script to run the Pipeline Wrapper,


import logging
from tasks import taskCoordinator
from config import settings


#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s')


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler("%s" % settings['log_path']),
        logging.StreamHandler()
    ])
TaskManager = taskCoordinator()
TaskManager.print_pipeline()
#TaskManager.run()
TaskManager.run()
exit(1)
