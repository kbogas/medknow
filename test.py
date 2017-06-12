#!/usr/bin/python !/usr/bin/env python
# -*- coding: utf-8 -*

import logging
from tasks import taskCoordinator

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s')
a = taskCoordinator()
a.print_pipeline()
a.run()