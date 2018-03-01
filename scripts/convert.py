#!/usr/bin/env python
import csv, sys, os
import logging, argparse
import multiprocessing as mp

logging.basicConfig(format='[%(asctime)s][%(levelname)s][%(funcName)s] - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def convert_csv_to_tsv(csv_in, tsv_out):
	logger.info("Converting: %s ..." % os.path.split(csv_in)[1])
	with open(csv_in, 'r') as fin:
		with open(tsv_out, 'w') as fout:
			csv.writer(fout, dialect='excel-tab').writerows(csv.reader(fin))
	logger.info("Completed %s" % os.path.split(csv_in)[1])


def parse_args():
    parser = argparse.ArgumentParser(description="CSV to TSV converter", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    io_options_group = parser.add_argument_group("I/O Options")
    io_options_group.add_argument('-i', "--input", help="Input directory")
    io_options_group.add_argument('-o', "--output", help="Output directory")

    options_group = parser.add_argument_group("I/O Options")
    options_group.add_argument('-b', '--batch-size', type=int, default=10, help="How many to process at once")

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
	if not os.path.exists(args.input):
		logger.error("Input directory: %s not found.")
		raise

	if not os.path.exists(args.output):
		logger.debug("Output directory: %d did not exist. Creating it...")
		os.makedirs(args.output)
	else:
		logger.debug("Output directory: %s" % args.output)

	input_csvs = [file for file in os.listdir(args.input) if file.endswith(".csv")]
	output_csvs = ["%s.tsv" % os.path.splitext(file)[0] for file in input_csvs]

	input_csvs = map(lambda p: os.path.join(args.input, p), input_csvs)
	output_csvs = map(lambda p: os.path.join(args.output, p), output_csvs)

	num_files = len(input_csvs)

	if input_csvs:
		logger.info("Found %d CSVs to convert" % num_files)
	else:
		logger.warning("Found no CSV files to convert. Exiting.")
		return

	logger.debug("Processing in batches of %d" % args.batch_size)
	procs = []
	for csv_in, tsv_out in zip(input_csvs, output_csvs):
		procs.append(mp.Process(target=convert_csv_to_tsv, args=[csv_in, tsv_out]))

	num_batches = 1 + num_files / args.batch_size
	for batch_index in xrange(num_batches):
		start = batch_index * args.batch_size
		end = min((1 + batch_index) * args.batch_size, num_files)
		for p in procs[start:end]: p.start()
		for p in procs[start:end]: p.join()

	logger.debug("Conversion complete.")

if __name__ == "__main__":
	main()