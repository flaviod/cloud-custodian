# Copyright 2016 Capital One Services, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import logging
import os
import pdb
import sys
import traceback
from dateutil.parser import parse as date_parse

from c7n import commands, resources


def _default_options(p, extended_options=True):
    p.add_argument(
        "-r", "--region",
        default=os.environ.get('AWS_DEFAULT_REGION', "us-east-1"),
        help="AWS Region to target (Default: us-east-1)")
    p.add_argument(
        "--profile",
        help="AWS Account Config File Profile to utilize")
    p.add_argument("--assume", default=None, dest="assume_role",
                   help="Role to assume")
    p.add_argument("-c", "--config", required=True,
                   help="Policy Configuration File")
    p.add_argument("-p", "--policies", default=None, dest='policy_filter',
                   help="Only execute named/matched policies")
    p.add_argument("-t", "--resource", default=None, dest='resource_type',
                   help="Only execute policies with the given resource type")

    p.add_argument("-v", "--verbose", action="store_true",
                   help="Verbose Logging")

    p.add_argument("--debug", action="store_true",
                   help="Dev Debug")

    if extended_options:
        _extended_default_options(p)


def _extended_default_options(p):
    p.add_argument(
        "-l", "--log-group", default=None,
        help="Cloudwatch Log Group to send policy logs")

    p.add_argument("-s", "--output-dir", required=True,
                   help="Directory or S3 URL For Policy Output")
    p.add_argument("-f", "--cache", default="~/.cache/cloud-custodian.cache")
    p.add_argument("--cache-period", default=60, type=int,
                   help="Cache validity in seconds (Default 60)")


def _policy_metrics_options(p):
    """ Add options specific to policy-metrics subcommand. """
    _default_options(p, extended_options=False)
    p.add_argument(
        '--start', type=date_parse, help='Start date (requires --end, overrides --days)')
    p.add_argument(
        '--end', type=date_parse, help='End date')
    p.add_argument(
        '--days', type=int, default=14,
        help='Number of days of history to consider (default: %(default)i)')
    p.add_argument('--period', type=int, default=60*24*24)

    # The original tools/policymetrics.py set these to None, so I am adding them
    # as hidden options so they are always None.
    p.add_argument("--log-group", default=None, help=argparse.SUPPRESS)
    p.add_argument("--cache", default=None, help=argparse.SUPPRESS)


def _policy_metrics_validate(parser, options):
    """ Validate options specified for policy-metrics subcommand. """

    # --start and --end must be specified together
    if bool(options.start) ^ bool(options.end):
        # Using `parser.exit` instead of `parser.error` because the latter one
        # will print the top-level usage message, instead of the policy-metrics
        # specific one.  I couldn't figure out how to get argparse to give me
        # the subparser class so I can't call the more appropriate
        # `subparser.error`.
        parser.exit(status=2, message='error: --start and --end must be specified together.\n')


def _dryrun_option(p):
    p.add_argument(
        "-d", "--dryrun", action="store_true",
        help="Don't change infrastructure but verify access.")


def _key_val_pair(value):
    """
    Type checker to ensure that --field values are of the format key=val
    """
    if '=' not in value:
        msg = 'values must be of the form `header=field`'
        raise argparse.ArgumentTypeError(msg)
    return value


def setup_parser():
    parser = argparse.ArgumentParser()
    
    # Setting `dest` means we capture which subparser was used.  We'll use it
    # later on when doing post-parsing validation.
    subs = parser.add_subparsers(dest='subparser')

    report = subs.add_parser("report")
    report.set_defaults(command=commands.report)
    _default_options(report)
    report.add_argument(
        '--days', type=float, default=1,
        help="Number of days of history to consider")
    report.add_argument(
        '--raw', type=argparse.FileType('wb'),
        help="Store raw json of collected records to given file path")
    report.add_argument(
        '--field', action='append', default=[], type=_key_val_pair,
        metavar='HEADER=FIELD',
        help='Repeatable. JMESPath of field to include in the output OR '\
            'for a tag use prefix `tag:`')
    report.add_argument(
        '--no-default-fields', action="store_true",
        help='Exclude default fields for report.')

    logs = subs.add_parser('logs')
    logs.set_defaults(command=commands.logs)
    _default_options(logs)

    policy_metrics = subs.add_parser('policy-metrics')
    policy_metrics.set_defaults(command=commands.policy_metrics)
    _policy_metrics_options(policy_metrics)

    version = subs.add_parser('version')
    version.set_defaults(command=commands.cmd_version)
    version.add_argument(
        "-v", "--verbose", action="store_true",
        help="Verbose Logging")

    validate = subs.add_parser('validate')
    validate.set_defaults(command=commands.validate)
    validate.add_argument(
        "-c", "--config",
        help="Policy Configuration File (old; use configs instead)"
    )
    validate.add_argument("configs", nargs='*',
                          help="Policy Configuration File(s)")
    validate.add_argument("-v", "--verbose", action="store_true",
                          help="Verbose Logging")
    validate.add_argument("--debug", action="store_true",
                          help="Dev Debug")

    schema = subs.add_parser('schema')
    schema.set_defaults(command=commands.schema)
    schema.add_argument('--summarize', action="store_true",
                        help="Summarize counts of available resources, \
                              actions and filters")
    schema.add_argument('--json', action="store_true",
                        help="Switch output to JSON")
    schema.add_argument("-v", "--verbose", action="store_true",
                        help="Verbose Logging")

    #resources = subs.add_parser('resources')
    #resources.set_defaults(command=commands.resources)
    #_default_options(resources)
    #resources.add_argument('--all', default=True, action="store_false")

    run = subs.add_parser("run")
    run.set_defaults(command=commands.run)
    _default_options(run)
    _dryrun_option(run)
    run.add_argument(
        "-m", "--metrics-enabled",
        default=False, action="store_true",
        help="Emit Metrics")

    return parser


def main():
    parser = setup_parser()
    options = parser.parse_args()

    # policy-metrics requires some post-parsing validation
    if options.subparser == 'policy-metrics':
        _policy_metrics_validate(parser, options)

    level = options.verbose and logging.DEBUG or logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s: %(name)s:%(levelname)s %(message)s")
    logging.getLogger('botocore').setLevel(logging.ERROR)
    logging.getLogger('s3transfer').setLevel(logging.ERROR)

    try:
        resources.load_resources()
        options.command(options)
    except Exception:
        if not options.debug:
            raise
        traceback.print_exc()
        pdb.post_mortem(sys.exc_info()[-1])

