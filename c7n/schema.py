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
"""
Jsonschema validation of cloud custodian config.

We start with a walkthrough of the various class registries
of resource types and assemble and generate the schema.

We do some specialization to reduce overall schema size
via reference usage, although in some cases we prefer
copies, due to issues with inheritance via reference (
allowedProperties and enum extension).

All filters and actions are annotated with schema typically using
the utils.type_schema function.
"""
from collections import Counter
import json
import logging
import yaml
import inspect

from jsonschema import Draft4Validator as Validator
from jsonschema.exceptions import best_match

from c7n.manager import resources
from c7n.resources import load_resources
from c7n.filters import ValueFilter, EventFilter, AgeFilter


def validate(data, schema=None):
    if schema is None:
        schema = generate()
        Validator.check_schema(schema)
    validator = Validator(schema)

    errors = list(validator.iter_errors(data))
    if not errors:
        counter = Counter([p['name'] for p in data.get('policies')])
        dupes = []
        for k, v in counter.items():
            if v > 1:
                dupes.append(k)
        if dupes:
            return [ValueError(
                "Only one policy with a given name allowed, duplicates: %s" % (
                    ", ".join(dupes)))]
        return []
    try:
        resp = specific_error(errors[0])
        name = isinstance(errors[0].instance, dict) and errors[0].instance.get('name', 'unknown') or 'unknown'
        return [resp, name]
    except Exception:
        logging.exception(
            "specific_error failed, traceback, followed by fallback")

    return filter(None, [
        errors[0],
        best_match(validator.iter_errors(data)),
    ])


def specific_error(error):
    """Try to find the best error for humans to resolve

    The jsonschema.exceptions.best_match error is based purely on a
    mix of a strong match (ie. not anyOf, oneOf) and schema depth,
    this often yields odd results that are semantically confusing,
    instead we can use a bit of structural knowledge of schema to
    provide better results.
    """
    if error.validator not in ('anyOf', 'oneOf'):
        return error

    r = t = None
    if isinstance(error.instance, dict):
        t = error.instance.get('type')
        r = error.instance.get('resource')

    if r is not None:
        found = None
        for idx, v in enumerate(error.validator_value):
            if r in v['$ref'].rsplit('/', 2):
                found = idx
        if found is not None:
            # error context is a flat list of all validation
            # failures, we have to index back to the policy
            # of interest.
            for e in error.context:
                # resource policies have a fixed path from
                # the top of the schema
                if e.absolute_schema_path[4] == found:
                    return specific_error(e)
            return specific_error(error.context[idx])

    if t is not None:
        found = None
        for idx, v in enumerate(error.validator_value):
            if '$ref' in v and v['$ref'].endswith(t):
                found = idx
        if found is not None:
            # Try to walk back an element/type ref to the specific
            # error
            spath = list(error.context[0].absolute_schema_path)
            spath.reverse()
            slen = len(spath)
            if 'oneOf' in spath:
                idx = spath.index('oneOf')
            elif 'anyOf' in spath:
                idx = spath.index('anyOf')
            vidx = slen - idx
            for e in error.context:
                if e.absolute_schema_path[vidx] == found:
                    return e
    return error


def generate(resource_types=()):
    resource_defs = {}
    definitions = {
        'resources': resource_defs,
        'filters': {
            'value': ValueFilter.schema,
            'event': EventFilter.schema,
            'age': AgeFilter.schema,
            # Shortcut form of value filter as k=v
            'valuekv': {
                'type': 'object',
                'minProperties': 1,
                'maxProperties': 1},
        },

        'policy': {
            'type': 'object',
            'required': ['name', 'resource'],
            'additionalProperties': False,
            'properties': {
                'name': {
                    'type': 'string',
                    'pattern': "^[A-z][A-z0-9]*(-[A-z0-9]*[A-z][A-z0-9]*)*$"},
                'region': {'type': 'string'},
                'resource': {'type': 'string'},
                'max-resources': {'type': 'integer'},
                'comment': {'type': 'string'},
                'comments': {'type': 'string'},
                'description': {'type': 'string'},
                'tags': {'type': 'array', 'items': {'type': 'string'}},
                'mode': {'$ref': '#/definitions/policy-mode'},
                'actions': {
                    'type': 'array',
                },
                'filters': {
                    'type': 'array'
                },
                #
                # unclear if this should be allowed, it kills resource
                # cache coherency between policies, and we need to
                # generalize server side query mechanisms, currently
                # this only for ec2 instance queries. limitations
                # in json schema inheritance prevent us from doing this
                # on a type specific basis http://goo.gl/8UyRvQ
                'query': {
                    'type': 'array', 'items': {
                        'type': 'object',
                        'minProperties': 1,
                        'maxProperties': 1}}
            },
        },
        'policy-mode': {
            'type': 'object',
            'required': ['type'],
            'properties': {
                'type': {
                    'enum': [
                        'cloudtrail',
                        'ec2-instance-state',
                        'asg-instance-state',
                        'config-rule',
                        'periodic'
                    ]},
                'events': {'type': 'array', 'items': {
                    'oneOf': [
                        {'type': 'string'},
                        {'type': 'object',
                         'required': ['event', 'source', 'ids'],
                         'properties': {
                             'source': {'type': 'string'},
                             'ids': {'type': 'string'},
                             'event': {'type': 'string'}}}]
                    }}
            },
        },
    }

    resource_refs = []
    for type_name, resource_type in resources.items():
        if resource_types and type_name not in resource_types:
            continue
        resource_refs.append(
            process_resource(type_name, resource_type, resource_defs))

    schema = {
        '$schema': 'http://json-schema.org/schema#',
        'id': 'http://schema.cloudcustodian.io/v0/custodian.json',
        'definitions': definitions,
        'type': 'object',
        'required': ['policies'],
        'additionalProperties': False,
        'properties': {
            'vars': {'type': 'object'},
            'policies': {
                'type': 'array',
                'additionalItems': False,
                'items': {'anyOf': resource_refs}
                }
            }
    }

    return schema


def process_resource(type_name, resource_type, resource_defs):
    r = resource_defs.setdefault(type_name, {'actions': {}, 'filters': {}})

    seen_actions = set()  # Aliases get processed once
    action_refs = []
    for action_name, a in resource_type.action_registry.items():
        if a in seen_actions:
            continue
        else:
            seen_actions.add(a)
        r['actions'][action_name] = a.schema
        action_refs.append(
            {'$ref': '#/definitions/resources/%s/actions/%s' % (
                type_name, action_name)})

    # one word action shortcuts
    action_refs.append(
        {'enum': resource_type.action_registry.keys()})

    nested_filter_refs = []
    filters_seen = set()
    for k, v in sorted(resource_type.filter_registry.items()):
        if v in filters_seen:
            continue
        else:
            filters_seen.add(v)
        nested_filter_refs.append(
            {'$ref': '#/definitions/resources/%s/filters/%s' % (
                type_name, k)})
    nested_filter_refs.append(
        {'$ref': '#/definitions/filters/valuekv'})

    filter_refs = []
    filters_seen = set() # for aliases
    for filter_name, f in sorted(resource_type.filter_registry.items()):
        if f in filters_seen:
            continue
        else:
            filters_seen.add(f)

        if filter_name in ('or', 'and'):
            continue
        elif filter_name == 'value':
            r['filters'][filter_name] = {
                '$ref': '#/definitions/filters/value'}
            r['filters']['valuekv'] = {
                '$ref': '#/definitions/filters/valuekv'}
        elif filter_name == 'event':
            r['filters'][filter_name] = {
                '$ref': '#/definitions/filters/event'}
        elif filter_name == 'or':
            r['filters'][filter_name] = {
                'type': 'array',
                'items': {'anyOf': nested_filter_refs}}
        elif filter_name == 'and':
            r['filters'][filter_name] = {
                'type': 'array',
                'items': {'anyOf': nested_filter_refs}}
        else:
            r['filters'][filter_name] = f.schema
        filter_refs.append(
            {'$ref': '#/definitions/resources/%s/filters/%s' % (
                type_name, filter_name)})
    filter_refs.append(
        {'$ref': '#/definitions/filters/valuekv'})

    # one word filter shortcuts
    filter_refs.append(
        {'enum': resource_type.filter_registry.keys()})

    resource_policy = {
        'allOf': [
            {'$ref': '#/definitions/policy'},
            {'properties': {
                'resource': {'enum': [type_name]},
                'filters': {
                    'type': 'array',
                    'items': {'anyOf': filter_refs}},
                'actions': {
                    'type': 'array',
                    'items': {'anyOf': action_refs}}}},
            ]
    }

    if type_name == 'ec2':
        resource_policy['allOf'][1]['properties']['query'] = {}

    r['policy'] = resource_policy
    return {'$ref': '#/definitions/resources/%s/policy' % type_name}


def resource_vocabulary():
    vocabulary = {}
    for type_name, resource_type in resources.items():
        docs = {'actions': {}, 'filters': {}}

        actions = []
        for action_name, cls in resource_type.action_registry.items():
            actions.append(action_name)
            docs['actions'][action_name] = inspect.getdoc(cls)

        filters = []
        for filter_name, cls in resource_type.filter_registry.items():
            filters.append(filter_name)
            docs['filters'][filter_name] = inspect.getdoc(cls)

        vocabulary[type_name] = {
            'filters': sorted(filters),
            'actions': sorted(actions),
            'docs': docs,
        }
    return vocabulary


def schema_summary(vocabulary):
    print "resource count: %d" % len(vocabulary)
    action_count = filter_count = 0

    common_actions = set(['notify', 'invoke-lambda'])
    common_filters = set(['value', 'and', 'or', 'event'])

    for rv in vocabulary.values():
        action_count += len(
            set(rv.get('actions', ())).difference(common_actions))
        filter_count += len(
            set(rv.get('filters', ())).difference(common_filters))
    print "unique actions: %d" % action_count
    print "common actions: %d" % len(common_actions)
    print "unique filters: %d" % filter_count
    print "common filters: %s" % len(common_filters)


def json_dump():
    load_resources()
    try:
        print(json.dumps(generate(), indent=2))
    except:
        import traceback, pdb, sys
        traceback.print_exc()
        pdb.post_mortem(sys.exc_info()[-1])


def print_schema(options):
    """
    Print information about the schema.
    """
    if options.json:
        json_dump()
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
        raise ValueError('{} is not a valid resource'.format(resource))

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
        raise ValueError("Valid choices are 'actions' and 'filters'.  You supplied '{}'".format(category))
    
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
        raise ValueError('{} is not in the {} list for resource {}'.format(item, category, resource))

    if len(components) == 3:
        docstring = resource_mapping[resource]['docs'][category][item]
        if docstring:
            print(docstring)
        else:
            print("No help is available for this item.")
        return

    # We received too much (e.g. s3.actions.foo.bar)
    raise ValueError("Invalid selector '{}'.  Max of 3 components in the "\
                     "format RESOURCE.CATEGORY.ITEM".format(options.resource))


if __name__ == '__main__':
    # dump our schema
    #
    # The canonical way to do this now is `custodian schema --json`, but I am
    # leaving in this __main__ section in case people are relying on the old
    # ability to just do:
    # 
    # $ python -m c7n.schema
    json_dump()
