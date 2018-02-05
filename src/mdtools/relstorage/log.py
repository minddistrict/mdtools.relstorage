import zodbupdate.main


def setup(args):
    zodbupdate.main.setup_logger(quiet=args.quiet, verbose=args.verbose)
