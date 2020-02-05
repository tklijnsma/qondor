# -*- coding: utf-8 -*-
import os
from .logger import setup_logger
logger = setup_logger()

from . import utils

BATCHMODE = False
if 'QONDOR_BATCHMODE' in os.environ:
    BATCHMODE = True

from .schedd import get_best_schedd, get_schedd_ads
from .preprocess import Preprocessor
from .submit import SHFile, Submitter
