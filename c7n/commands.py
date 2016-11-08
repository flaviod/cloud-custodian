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
from datetime import timedelta, datetime
from functools import wraps
import json
import logging
import os
import sys
import time

import yaml

from c7n.credentials import SessionFactory
from c7n.policy import Policy, load as policy_load
from c7n.reports import report as do_report
from c7n.policymetrics import policy_metrics as do_policy_metrics
from c7n.utils import Bag
from c7n.exceptions import ArgumentError
from c7n.manager import resources
from c7n.resources import load_resources
from c7n.schema import json_dump as schema_json_dump
from c7n.schema import validate as schema_validate
from c7n.schema import resource_vocabulary, schema_summary
from c7n import mu, version


log = logging.getLogger('custodian.commands')


def policy_command(f):

    @wraps(f)
    def _load_policies(options):
        collection = policy_load(options, options.config)
        policies = collection.filter(options.policy_filter)
        return f(options, policies)

    return _load_policies


def validate(options):
    if options.config is not None:
        # support the old -c option
        options.configs.append(options.config)
    if len(options.configs) < 1:
        # no configs to test
        # We don't have the parser object, so fake ArgumentParser.error
        print('custodian validate: error: no config files specified')
        sys.exit(2)
    used_policy_names = set()
    for config_file in options.configs:
        if not os.path.exists(config_file):
            raise ValueError("Invalid path for config %r" % config_file)

        options.dryrun = True
        format = config_file.rsplit('.', 1)[-1]
        with open(config_file) as fh:
            if format in ('yml', 'yaml'):
                data = yaml.safe_load(fh.read())
            if format in ('json',):
                data = json.load(fh)

        errors = schema_validate(data)
        conf_policy_names = {p['name'] for p in data.get('policies', ())}
        dupes = conf_policy_names & used_policy_names
        if len(dupes) >= 1:
            errors.append(ValueError(
                "Only one policy with a given name allowed, duplicates: %s" % (
                    ", ".join(dupes)
                )
            ))
        used_policy_names = used_policy_names | conf_policy_names
        if not errors:
            null_config = Bag(dryrun=True, log_group=None, cache=None, assume_role="na")
            for p in data.get('policies', ()):
                try:
                    Policy(p, null_config, Bag())
                except Exception as e:
                    log.error("Policy: %s is invalid: %s" % (
                        p.get('name', 'unknown'), e))
                    sys.exit(1)
                    return
            log.info("Configuration valid: {}".format(config_file))
            continue

        log.error("Configuration invalid: {}".format(config_file))
        for e in errors:
            log.error(" %s" % e)
        sys.exit(1)


@policy_command
def run(options, policies):
    exit_code = 0
    for policy in policies:
        try:
            policy()
        except Exception:
            exit_code = 1
            if options.debug:
                raise
            # Output does an exception log
            log.warning("Error while executing policy %s, continuing" % (
                policy.name))
    sys.exit(exit_code)


@policy_command
def report(options, policies):
    assert len(policies) == 1, "Only one policy report at a time"
    policy = policies.pop()
    d = datetime.now()
    delta = timedelta(days=options.days)
    begin_date = d - delta
    do_report(
        policy, begin_date, options, sys.stdout,
        raw_output_fh=options.raw)


@policy_command
def logs(options, policies):
    assert len(policies) == 1, "Only one policy log at a time"
    policy = policies.pop()

    if not policy.is_lambda:
        log.debug('lambda only atm')
        return

    session_factory = SessionFactory(
        options.region, options.profile, options.assume_role)
    manager = mu.LambdaManager(session_factory)
    for e in manager.logs(mu.PolicyLambda(policy)):
        print "%s: %s" % (
            time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(e['timestamp'] / 1000)),
            e['message'])


def schema(options):
    """
    Output information about the resources, actions and filters available.
    """
    if options.json:
        schema_json_dump(options.resource)
        return
        
    load_resources()
    resource_mapping = resource_vocabulary()

    if options.summary:
        schema_summary(resource_mapping)
        return

    # Here are the formats for what we accept:
    # - No argument
    #   - List all available RESOURCES
    # - RESOURCE
    #   - List all available actions and filters for supplied RESOURCE
    # - RESOURCE.actions
    #   - List all available actions for supplied RESOURCE
    # - RESOURCE.actions.ACTION
    #   - Show class doc string and schema for supplied action
    # - RESOURCE.filters
    #   - List all available filters for supplied RESOURCE
    # - RESOURCE.filters.FILTER
    #   - Show class doc string and schema for supplied filter

    if not options.resource:
        resource_list = {'resources': sorted(resources.keys()) }
        print(yaml.safe_dump(resource_list, default_flow_style=False))
        return

    # Format is RESOURCE.CATEGORY.ITEM
    components = options.resource.split('.')

    #
    # Handle resource
    #
    resource = components[0].lower()
    if resource not in resource_mapping:
        print('{} is not a valid resource'.format(resource))
        sys.exit(2)

    if len(components) == 1:
        del(resource_mapping[resource]['docs'])
        output = {resource: resource_mapping[resource]}
        print(yaml.safe_dump(output))
        return

    #
    # Handle category
    #
    category = components[1].lower()
    if category not in ('actions', 'filters'):
        print("Valid choices are 'actions' and 'filters'.  You supplied '{}'".format(category))
        sys.exit(2)
    
    if len(components) == 2:
        output = "No {} available for resource {}.".format(category, resource)
        if category in resource_mapping[resource]:
            output = {resource: {category: resource_mapping[resource][category]}}
        print(yaml.safe_dump(output))
        return

    #
    # Handle item
    #
    item = components[2].lower()
    if item not in resource_mapping[resource][category]:
        print('{} is not in the {} list for resource {}'.format(item, category, resource))
        sys.exit(2)

    if len(components) == 3:
        docstring = resource_mapping[resource]['docs'][category][item]
        if docstring:
            print(docstring)
        else:
            print("No help is available for this item.")
        return

    # We received too much (e.g. s3.actions.foo.bar)
    print("Invalid selector '{}'.  Max of 3 components in the "\
          "format RESOURCE.CATEGORY.ITEM".format(options.resource))
    sys.exit(2)
    

@policy_command
def policy_metrics(options, policies):
    try:
        do_policy_metrics(options, policies)
    except ArgumentError as e:
        print("Error: " + e.message)
        sys.exit(2)


def cmd_version(options):
    print(version.version)
