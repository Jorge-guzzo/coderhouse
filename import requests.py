import pip
import json

import os
from configparser import ConfigParser

from pathlib import Path

config = ConfigParser()

config_dir = "config/config.ini"

config.read(config_dir)
