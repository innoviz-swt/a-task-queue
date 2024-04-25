import argparse

from ataskq import TaskQ
from ataskq.env import ATASKQ_CONNECTION


def main(args=None):
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(title="commands", dest="command")
    run_p = subparsers.add_parser("run")

    run_p.add_argument("--connection", "-c", help="connection string", default=ATASKQ_CONNECTION)
    run_p.add_argument("--job-id", "-jid", type=int, help="job id to run")
    run_p.add_argument("--level", "-l", type=int, nargs=2, help="job level to run")

    args = parser.parse_args(args=args)
    if args.command == "run":
        TaskQ(conn=args.connection, job_id=args.job_id).run(level=args.level)


if __name__ == "__main__":
    main()
