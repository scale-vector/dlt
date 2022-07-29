import jsonlines
from datetime import date, datetime  # noqa: I251
from typing import Any, Iterable, Literal, Sequence, IO

from dlt.common import json
from dlt.common.typing import StrAny

TLoaderFileFormat = Literal["jsonl", "insert_values"]


def write_jsonl(f: IO[Any], rows: Sequence[Any]) -> None:
    # use jsonl to write load files https://jsonlines.org/
    with jsonlines.Writer(f, dumps=json.dumps) as w:
        w.write_all(rows)


def write_insert_values(f: IO[Any], rows: Sequence[StrAny], headers: Iterable[str]) -> None:
    # dict lookup is always faster
    headers_lookup = {v: i for i, v in enumerate(headers)}
    # do not write INSERT INTO command, this must be added together with table name by the loader
    f.write("INSERT INTO {}(")
    f.write(",".join(map(escape_redshift_identifier, headers)))
    f.write(")\nVALUES\n")

    def stringify(v: Any) -> str:
        if isinstance(v, bytes):
           return f"from_hex('{v.hex()}')"
        if isinstance(v, (datetime, date)):
            return escape_redshift_literal(v.isoformat())
        else:
            return str(v)

    def write_row(row: StrAny) -> None:
        output = ["NULL" for _ in range(len(headers_lookup))]
        for n,v  in row.items():
            output[headers_lookup[n]] = escape_redshift_literal(v) if isinstance(v, str) else stringify(v)
        f.write("(")
        f.write(",".join(output))
        f.write(")")

    for row in rows[:-1]:
        write_row(row)
        f.write(",\n")

    write_row(rows[-1])
    f.write(";")


def escape_redshift_literal(v: str) -> str:
    # https://www.postgresql.org/docs/9.3/sql-syntax-lexical.html
    # looks like this is the only thing we need to escape for Postgres > 9.1
    # redshift keeps \ as escape character which is pre 9 behavior
    return "'" + v.replace("'", "''").replace("\\", "\\\\") + "'"


def escape_redshift_identifier(v: str) -> str:
    return '"' + v.replace('"', '""').replace("\\", "\\\\") + '"'


def escape_bigquery_identifier(v: str) -> str:
    # https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical
    return "`" + v.replace("\\", "\\\\").replace("`","\\`") + "`"
