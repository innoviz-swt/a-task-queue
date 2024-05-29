import argparse

from ataskq import TaskQ
from ataskq.env import ATASKQ_CONFIG


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
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(title="commands", dest="command")
    run_p = subparsers.add_parser("run")

    run_p.add_argument("--config", "-c", help="config set or path to file", default=ATASKQ_CONFIG)
    run_p.add_argument("--job-id", "-jid", type=int, help="job id to run")
    run_p.add_argument("--level", "-l", type=int, nargs="+", help="job level to run")
    run_p.add_argument(
        "--concurrency", "-cn", type=parse_number, help="number of task execution processes to run in parallel"
    )

    args = parser.parse_args(args=args)

    # specific args handling
    if args.level is not None and len(args.level) == 1:
        args.level = args.level[0]

    if args.command == "run":
        TaskQ(config=args.config, job_id=args.job_id).run(level=args.level, concurrency=args.concurrency)


if __name__ == "__main__":
    main()
