import argparse
import logging

from ataskq import TaskQ
from ataskq.config.config import CONFIG_SETS, DEFAULT_CONFIG


def init_logger(level=logging.INFO):
    logger = logging.getLogger("ataskq")

    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d [%(process)d] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    handler.setLevel(level)

    logger.addHandler(handler)
    logger.setLevel(level)

    return logger


def parse_number(number: str):
    try:
        ret = int(number)
        return ret
    except ValueError:
        pass

    try:
        ret = float(number)
        return ret
    except ValueError:
        raise ValueError(f"Failed to parse '{number}'")


def main(args=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter, description="ataskq command line interface help"
    )
    subparsers = parser.add_subparsers(title="commands", dest="command")
    run_p = subparsers.add_parser("run")

    run_p.add_argument(
        "--config",
        "-c",
        help=f"config preset {list(CONFIG_SETS.keys())} or path to file",
        default=DEFAULT_CONFIG,
    )
    run_p.add_argument("--job-id", "-jid", type=int, help="job id to run")
    run_p.add_argument("--level", "-l", type=int, nargs="+", help="job level to run")
    run_p.add_argument(
        "--concurrency", "-cn", type=parse_number, help="number of task execution processes to run in parallel"
    )

    args = parser.parse_args(args=args)

    # specific args handling
    if args.command == "run":
        if args.level is not None and len(args.level) == 1:
            args.level = args.level[0]
        init_logger()
        TaskQ(config=args.config, job_id=args.job_id).run(level=args.level, concurrency=args.concurrency)


if __name__ == "__main__":
    main()
