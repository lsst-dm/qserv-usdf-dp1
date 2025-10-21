#!/usr/bin/env python3
  
import argparse
import json as json
import sys
import time

from ingest_api import ingest_api
from ingest_util import fatal, info, argument_parser

def parseArguments():
    parser = argument_parser(
        """Start a transaction. Get table locations at all workers. Initiate ingestion of
        the table contributions using ASYNC method. Wait before the completion of the operation.
        Commit the transaction.""")
    parser.add_argument(
        "--database",
        """The required name of a database.
        The database should exist and it should not be published.""")
    parser.add_argument(
        "--table",
        """The required name of a table where the contributions will be ingsted.
        The table should exist and it should not be published.""")
    parser.add_argument(
        "--fields-terminated-by",
        "Field separator that matches the CSV dialect of the contributions.",
        "\\t")
    parser.add_argument(
        "--fields-enclosed-by",
        """The optional character for quoting the fields. It should match the CSV dialect
        of the contributions.""",
        "")
    parser.add_argument(
        "--url",
        "A location of the contribution.")

    args = parser.parse_args()
    if args.database is None or args.database == "":
        fatal("The database name is required.")
    if args.table is None or args.table == "":
        fatal("The table name is required.")
    if args.fields_terminated_by is None or args.fields_terminated_by == "":
        fatal("The field terminator is required.")
    if args.url is None or args.url == "":
        fatal("The URL of the contribution is required.")

    return args

def get_chunk_location(api, database, chunk):
    chunks = set([chunk,])
    locations = api.locate_chunks(database, chunks)
    if chunk not in locations:
        fatal("Incorect location reported for chunk={}".format(chunk))
    return locations[chunk]

if __name__ == '__main__':

    args = parseArguments()

    api = ingest_api(args.qserv_config, args.debug)
    itable_locations = api.locate_regular_tables(args.database)
    trans_id = api.start_trans(args.database)
    if args.verbose:
        info("TRANS:     {}\tSTARTED".format(trans_id))

    contrib_descr = {
        "transaction_id":       trans_id,
        "table":                args.table,
        "fields_terminated_by": args.fields_terminated_by,
        "fields_enclosed_by":   args.fields_enclosed_by,
        "chunk":                0,
        "overlap":              0,
        "url":                  args.url}

    contrib_entries = {}
    for location in itable_locations:
        contrib = api.async_contrib(location, contrib_descr)
        contrib_id = contrib["id"]
        contrib_entries[contrib_id] = {
            "location": location,
            "contrib": contrib,
        };
        if args.verbose:
            info("CONTRIB:   {}\t{}\tworker={}".format(contrib_id, contrib["status"], location["worker"]))

    failed_contrib_entries = {}

    while len(contrib_entries) != 0:
        for contrib_id in list(contrib_entries.keys()):
            contrib = api.async_contrib_status(contrib_entries[contrib_id]["location"], contrib_id)
            contrib_entries[contrib_id]["contrib"] = contrib
            status = contrib["status"]
            if status == "IN_PROGRESS":
                entry = contrib_entries[contrib_id]
            else:
                entry = contrib_entries.pop(contrib_id)
                if status != "FINISHED":
                    failed_contrib_entries[contrib_id] = entry
            if args.verbose:
                info("CONTRIB:   {}\t{}\tworker={}".format(contrib_id, status, entry["location"]["worker"]))
        time.sleep(1)

    num_failed_contrib = len(failed_contrib_entries)
    if num_failed_contrib != 0:
        api.abort_trans(trans_id)
        if args.verbose:
            info("TRANS:     {}\tABORTED".format(trans_id))
        fatal("failed_contribs={}".format([contrib_id for contrib_id in failed_contrib_entries.keys()]))
    else:
        api.commit_trans(trans_id)
        if args.verbose:
            info("TRANS:     {}\tCOMMITTED".format(trans_id))

