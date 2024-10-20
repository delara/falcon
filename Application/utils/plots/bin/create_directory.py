#!/usr/bin/env python

import sys;
from os import sep, path

sys.path.insert(0, sys.path[0][0:sys.path[0].rfind(sep)])
from lib.utils.utils import parse_options
from lib.create_plots import create_plots_directory

opts = parse_options()
if path.exists(opts.input):
    create_plots_directory(input_dir=opts.input)
else:
    raise NameError("Input file does not exist! Input file: " + opts.input)
