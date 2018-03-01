#!/usr/bin/env python
import csv, sys, os
import logging, argparse
import multiprocessing as mp

logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def convert_csv_to_tsv(csv_in, tsv_out):
	logging.info("Converting: %s ..." % csv_in)
	with open(csv_in, 'r') as fin:
		with open(tsv_out, 'w') as fout:
			csv.writer(fout, dialect='excel-tab').writerows(csv.reader(fin))


def parse_args():
    parser = argparse.ArgumentParser(description="CSV to TSV converter", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    io_options_group = parser.add_argument_group("I/O Options")
    io_options_group.add_argument('-i', "--input", help="Input directory")
    io_options_group.add_argument('-o', "--output", help="Output directory")

    console_options_group = parser.add_argument_group("Console Options")
    console_options_group.add_argument('-v', '--verbose', action='store_true', help='verbose output')
    console_options_group.add_argument('--debug', action='store_true', help='Debug Console')

    args = parser.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)
        logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
    elif args.verbose:
        logger.setLevel(logging.INFO)
        logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
    else:
        logger.setLevel(logging.WARNING)
        logging.basicConfig(format='[log][%(levelname)s] - %(message)s')
    return args


def main():
	args = parse_args()

	logger.debug("Input directory: %s" % args.input)
	logger.debug("Output directory: %s" % args.output)

	if not os.path.exists(args.input):
		logger.error("Input directory: %s not found.")
		raise

	if not os.path.exists(args.output):
		logger.debug("Output directory: %d did not exist. Creating it...")
		os.makedirs(args.output)

	input_csvs = [file for file in os.listdir(args.input) if file.endswith(".csv")]
	output_csvs = ["%s.tsv" % os.path.splitext(file)[0] for file in input_csvs]

	input_csvs = list(map(lambda p: os.path.join(args.input, p), input_csvs))
	output_csvs = list(map(lambda p: os.path.join(args.output, p), output_csvs))

	logger.info("%d CSVs found to convert" % len(input_csvs))

	procs = []
	for csv_in, tsv_out in zip(input_csvs, output_csvs):
		procs.append(mp.Process(target=convert_csv_to_tsv, args=[csv_in, tsv_out]))

	for p in procs: p.start()
	for p in procs: p.join()

	logger.debug("Conversion complete.")

if __name__ == "__main__":
	main()