import yaml
import os
import argparse
from typing import Callable

from dlt.common import json
from dlt.common.schema import Schema
from dlt.common.runners.pool_runner import TRunArgs, add_pool_cli_arguments
from dlt.common.typing import DictStrAny
from dlt.common.utils import str2bool


def str2bool_a(v: str) -> bool:
    try:
        return str2bool(v)
    except ValueError:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def main() -> None:
    parser = argparse.ArgumentParser(description="Runs various DLT modules", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest="command")
    normalize = subparsers.add_parser("normalize", help="Runs normalize")
    add_pool_cli_arguments(normalize)
    load = subparsers.add_parser("load", help="Runs loader")
    add_pool_cli_arguments(load)
    dbt = subparsers.add_parser("dbt", help="Executes dbt package")
    add_pool_cli_arguments(dbt)
    schema = subparsers.add_parser("schema", help="Shows, converts and upgrades schemas")
    schema.add_argument("file", help="Schema file name, in yaml or json format, will autodetect based on extension")
    schema.add_argument("--format", choices=["json", "yaml"], default="yaml", help="Display schema in this format")
    schema.add_argument("--remove-defaults", action="store_true", help="Does not show default hint values")

    # TODO: consider using fire: https://github.com/google/python-fire
    args = parser.parse_args()
    run_f: Callable[[TRunArgs], None] = None

    if args.command == "normalize":
        from dlt.normalize.normalize import run_main as normalize_run
        run_f = normalize_run
    elif args.command == "load":
        from dlt.loaders.loader import run_main as loader_run
        run_f = loader_run
    elif args.command == "dbt":
        from dlt.dbt_runner.runner import run_main as dbt_run
        run_f = dbt_run
    elif args.command == "schema":
        with open(args.file, "r", encoding="utf-8") as f:
            if os.path.splitext(args.file)[1][1:] == "json":
                schema_dict: DictStrAny = json.load(f)
            else:
                schema_dict = yaml.safe_load(f)
        s = Schema.from_dict(schema_dict)
        if args.format == "json":
            schema_str = json.dumps(s.to_dict(remove_defaults=args.remove_defaults), indent=2)
        else:
            schema_str = s.as_yaml(remove_defaults=args.remove_defaults)
        print(schema_str)
        exit(0)
    else:
        parser.print_help()
        exit(-1)

    run_args = TRunArgs(args.single_run, args.wait_runs)
    run_f(run_args)

if __name__ == "__main__":
    main()
