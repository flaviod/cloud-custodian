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
from datetime import datetime, timedelta
import logging

from c7n.credentials import SessionFactory
from c7n.exceptions import ArgumentError
from c7n.resources import load_resources
from c7n.utils import dumps

log = logging.getLogger('custodian.policymetrics')


def policy_metrics(options, policies):

    # Are these lines needed?
    factory = SessionFactory(
        options.region, options.profile, options.assume_role)
    session = factory()
    client = session.client('cloudwatch')

    load_resources()
    start, end = get_endpoints(options)

    data = {}
    for p in policies:
        log.info('Getting %s metrics', p)
        data[p.name] = p.get_metrics(start, end, options.period)

    print dumps(data, indent=2)


def get_endpoints(options):
    """
    Determine the start and end dates based on the user-supplied optins.
    """
    if bool(options.start) ^ bool(options.end):
        raise ArgumentError('--start and --end must be specified together')

    if options.start and options.end:
        start = options.start
        end = options.end
    else:
        end = datetime.utcnow()
        start = end - timedelta(options.days)

    return start, end
