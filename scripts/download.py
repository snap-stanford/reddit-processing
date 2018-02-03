#!/usr/bin/env python

import os
import shutil as sh
from sh import rsync

import logging
import coloredlogs
import warnings

import argparse
import logging

logger = logging.getLogger(__name__)

class Downloader:

    def __init__(self, host, ):
        pass

    def download(self):
        pass

def main():
    downloader = parse_cli_args()
    downloader.download()

    



def parse_cli_args():

    script_description = 'Downloads data from server'
    parser = argparse.ArgumentParser(description=script_description, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    input_group = parser.add_argument_group("Inputs")
    input_group.add_argument('number')
    input_group.add_argument('host', help="host to download from")

    console_options_group = parser.add_argument_group("Console Options")
    console_options_group.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    console_options_group.add_argument('--debug', action='store_true', help='Debug console')

    args = parser.parse_args()

    if args.debug:
        coloredlogs.install(level='DEBUG')
    elif args.verbose:
        warnings.filterwarnings('ignore')
        coloredlogs.install(level='INFO')
    else:
        warnings.filterwarnings('ignore')
        coloredlogs.install(level='WARNING')

    downloader = Downloader()
    return downloader



if __name__ == "__main__": main()
