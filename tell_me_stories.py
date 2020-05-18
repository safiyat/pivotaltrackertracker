import argparse
from copy import deepcopy
import csv
from datetime import datetime as dt
from datetime import timedelta as td
import json
import os
import re
import requests
import sys
from tabulate import tabulate
import tempfile
from yaml import dump as yaml_dump


class PivotalTrackerEndpoints(object):
    STORIES_URL = \
        'https://www.pivotaltracker.com/services/v5/projects/{PROJECT_ID}/' \
        'stories'
    STORY_TRANSITIONS_URL = \
        'https://www.pivotaltracker.com/services/v5/projects/{PROJECT_ID}/' \
        'stories/{STORY_ID}/transitions'
    MEMBERSHIPS_URL = \
        'https://www.pivotaltracker.com/services/v5/projects/{PROJECT_ID}/' \
        'memberships'


class PTE(PivotalTrackerEndpoints):
    # Just an alias.
    pass


class PivotalTrackerConstants(object):
    ACCEPTED = 'accepted'
    ACCEPTED_AT = 'accepted_at'
    AND = 'AND'
    CURRENT_STATE = 'current_state'
    CYCLE_TIME = 'cycle_time'
    CYCLE_TIME_DETAILS = 'cycle_time_details'
    DELIVERED_TIME = 'delivered_time'
    ESTIMATE = 'estimate'
    FALSE = 'false'
    FIELDS = 'fields'
    FILTER = 'filter'
    FINISHED_TIME = 'finished_time'
    ID = 'id'
    INCLUDEDONE = 'includedone'
    KIND = 'kind'
    LABEL = 'label'
    LABELS = 'labels'
    NAME = 'name'
    OCCURRED_AT = 'occurred_at'
    OWNERS = 'owners'
    OWNER_IDS = 'owner_ids'
    PERFORMED_BY = 'performed_by'
    PERFORMED_BY_ID = 'performed_by_id'
    PERSON = 'person'
    PROJECT_ID = 'project_id'
    PROJECT_VERSION = 'project_version'
    STARTED = 'started'
    STARTED_TIME = 'started_time'
    STATE = 'state'
    STORY_ID = 'story_id'
    STORY_TYPE = 'story_type'
    TOTAL_CYCLE_TIME = 'total_cycle_time'
    TRACKER_TOKEN = 'X-TrackerToken'
    TRANSITIONS = 'transitions'
    TRUE = 'true'
    TYPE = 'type'
    UPDATED_AFTER = 'updated_after'
    UPDATED_BEFORE = 'updated_before'
    URL = 'url'

class PTC(PivotalTrackerConstants):
    # Just an alias.
    pass


class PivotalTrackerTrackerConstants(object):
    FINAL_CYCLE_TIME = 'final_cycle_time'
    DEVELOPMENT_TIME = 'development_time'
    REVIEW_PROCESS_TIME = 'review_process_time'
    ACCEPTANCE_PROCESS_TIME = 'acceptance_process_time'


class PTTC(PivotalTrackerTrackerConstants):
    # Just an alias.
    pass


def get_dict_from_list(list_of_dict, key, value):
    return list(filter(
        lambda dictionary: dictionary[key] == value, list_of_dict))


class PivotalTrackerStoriesFetcher(object):
    def __init__(self, token, project_id, *args, **kwargs):
        super(PivotalTrackerStoriesFetcher, self).__init__(*args, **kwargs)
        self.__token = token
        self.__project_id = project_id

    def _get_headers(self):
        headers = {PTC.TRACKER_TOKEN: self.__token}
        return headers

    def _make_request(self, url, params=None, headers=None):
        headers = headers or self._get_headers()
        response = requests.get(url, params=params, headers=headers)
        return response

    def _filter_stories_by_labels(self, stories, labels):
        filtered_stories = deepcopy(stories)
        for label in labels:
            latest_filtered_stories = list()
            for story in filtered_stories:
                story_labels = [l[PTC.NAME] for l in story.get(PTC.LABELS, [])]
                if label not in story_labels:
                    continue
                latest_filtered_stories.append(story)
            filtered_stories = latest_filtered_stories
        return filtered_stories

    def __build_filter(
            self, labels=None, updated_after=None, updated_before=None):
        # valid_filters = [PTC.LABEL, PTC.TYPE, PTC.INCLUDEDONE,
        #                  PTC.UPDATED_BEFORE, PTC.UPDATED_AFTER]

        filters = list()

        if labels:
            # label_filter = '%s:"%s"' % (PTC.LABEL, labels[0])
            label_filters = list()
            for label in labels:
                label_filters.append('%s:"%s"' % (PTC.LABEL, label))
            # label_filter = (' %s ' % PTC.AND).join(label_filters)
            label_filter = ' '.join(label_filters)
            filters.append(label_filter)

        if updated_after:
            filters.append('%s:"%s"' % (PTC.UPDATED_AFTER, updated_after))

        if updated_before:
            filters.append('%s:"%s"' % (PTC.UPDATED_BEFORE, updated_before))

        # Always include done stories.
        filters.append('%s:%s' % (PTC.INCLUDEDONE, str(PTC.TRUE).lower()))

        # filter_dict = \
        #     {PTC.FILTER: '(%s)' % (') %s (' % PTC.AND).join(filters)}
        filter_dict = {PTC.FILTER: ' '.join(filters)}
        return filter_dict

    def __build_fields(self, fields):
        if not fields:
            # An empty dict to a call to dict.update() doesn't break.
            return dict()
        fields_dict = {PTC.FIELDS: ','.join(fields)}
        return fields_dict

    def _prune_stories(self, stories, users):
        pruned_stories = deepcopy(stories)
        for story in pruned_stories:
            story[PTC.LABELS] = \
                [label[PTC.NAME] for label in story.pop(PTC.LABELS)]
            story[PTC.OWNERS] = \
                [get_dict_from_list(users, PTC.ID, owner_id)[0][PTC.NAME]
                 for owner_id in story.pop(PTC.OWNER_IDS)]
            if PTC.ESTIMATE not in story:
                story[PTC.ESTIMATE] = '-'
            for transition in story[PTC.TRANSITIONS]:
                user = get_dict_from_list(
                    users, PTC.ID, transition.pop(PTC.PERFORMED_BY_ID))
                user = user or [{PTC.NAME: 'Inactive User'}]
                transition[PTC.PERFORMED_BY] = user[0][PTC.NAME]
                transition.pop(PTC.STORY_ID)
                transition.pop(PTC.PROJECT_ID)
                transition.pop(PTC.PROJECT_VERSION)
                transition.pop(PTC.KIND)
            story[PTC.TRANSITIONS] = sorted(
                story.pop(PTC.TRANSITIONS),
                key=lambda k: dt.fromisoformat(k[PTC.OCCURRED_AT][:-1]))

            starts = get_dict_from_list(
                story[PTC.TRANSITIONS], PTC.STATE, PTC.STARTED)
            acceptances = get_dict_from_list(
                story[PTC.TRANSITIONS], PTC.STATE, PTC.ACCEPTED)
            if starts and acceptances:
                story[PTC.CYCLE_TIME_DETAILS][PTTC.FINAL_CYCLE_TIME] = str(
                    dt.fromisoformat(acceptances[-1][PTC.OCCURRED_AT][:-1]) -
                    dt.fromisoformat(starts[-1][PTC.OCCURRED_AT][:-1]))
            else:
                story[PTC.CYCLE_TIME_DETAILS][PTTC.FINAL_CYCLE_TIME] = '0:00:00'

            story_ctd = story[PTC.CYCLE_TIME_DETAILS]
            if PTC.TOTAL_CYCLE_TIME in story_ctd:
                story_ctd[PTC.TOTAL_CYCLE_TIME] = \
                    str(td(seconds=story_ctd.pop(PTC.TOTAL_CYCLE_TIME) / 1000))
            else:
                story_ctd[PTC.TOTAL_CYCLE_TIME] = '0:00:00'
            if PTC.STARTED_TIME in story_ctd:
                story_ctd[PTTC.DEVELOPMENT_TIME] = \
                    str(td(seconds=story_ctd.pop(PTC.STARTED_TIME) / 1000))
            else:
                story_ctd[PTC.STARTED_TIME] = '0:00:00'
            if PTC.FINISHED_TIME in story_ctd:
                story_ctd[PTTC.REVIEW_PROCESS_TIME] = \
                    str(td(seconds=story_ctd.pop(PTC.FINISHED_TIME) / 1000))
            else:
                story_ctd[PTC.FINISHED_TIME] = '0:00:00'
            if PTC.DELIVERED_TIME in story_ctd:
                story_ctd[PTTC.ACCEPTANCE_PROCESS_TIME] = \
                    str(td(seconds=story_ctd.pop(PTC.DELIVERED_TIME) / 1000))
            else:
                story_ctd[PTC.DELIVERED_TIME] = '0:00:00'

            story[PTC.CYCLE_TIME_DETAILS].pop(PTC.KIND, None)
            story[PTC.CYCLE_TIME_DETAILS].pop(PTC.STORY_ID, None)
            story[PTC.CYCLE_TIME_DETAILS].pop('', None)
            story[PTC.CYCLE_TIME_DETAILS].pop('', None)
            story[PTC.CYCLE_TIME_DETAILS].pop('', None)
        return pruned_stories

    def _flatten_stories(self, stories):
        flattened_stories = deepcopy(stories)
        for story in flattened_stories:
            story.update(story.pop(PTC.CYCLE_TIME_DETAILS, {}))
            flattened_transitions = list()
            for transition in story[PTC.TRANSITIONS]:
                flattened_transitions.append(
                    ", ".join(["%s:%s" % (k, v) for k, v in
                               transition.items()]))
            story[PTC.TRANSITIONS] = flattened_transitions

        return flattened_stories

    def fetch_stories(
            self, labels=None, updated_after=None, updated_before=None,
            fields=None, prune=True):
        default_fields = \
            [PTC.CURRENT_STATE, PTC.CYCLE_TIME_DETAILS, PTC.ESTIMATE, PTC.ID,
             PTC.LABELS, PTC.NAME, PTC.OWNER_IDS, PTC.STORY_TYPE,
             PTC.TRANSITIONS, PTC.URL]

        params = dict()

        params.update(
            self.__build_filter(labels, updated_after, updated_before))
        params.update(self.__build_fields(fields or default_fields))

        # print(params)
        response = self._make_request(
            PTE.STORIES_URL.format(PROJECT_ID=self.__project_id), params=params)
        stories = json.loads(response.text)

        # only_labels = [', '.join([l['name'] for l in
        #                           story['labels']]) for story in stories]

        if prune:
            response = self._make_request(
                PTE.MEMBERSHIPS_URL.format(PROJECT_ID=self.__project_id))
            users = [membership[PTC.PERSON] for membership in
                     json.loads(response.text)]
            stories = self._prune_stories(stories, users)
            stories = self._flatten_stories(stories)
        return stories


class CommandProcessor(object):
    def __init__(self, *args, **kwargs):
        super(CommandProcessor, self).__init__(*args, **kwargs)
        self.parser = None

    def _get_parser(self):
        def valid_date(string):
            if len(string) != 10:
                raise argparse.ArgumentTypeError(
                    'Invalid date string. The date string should be of the '
                    'format YYYY-MM-DD 1.')

            if not (string[4] == string[7] == '-'):
                raise argparse.ArgumentTypeError(
                    'Invalid date string. The date string should be of the '
                    'format YYYY-MM-DD 2.')

            year, month, day = \
                int(string[:4]), int(string[5:7]), int(string[8:10])
            if not all([year > 2015, year < (dt.now().year + 1),
                   month > 0, month < (dt.now().month + 1),
                   day > 0, day < 32]):
                raise argparse.ArgumentTypeError(
                    'Invalid date string. The date should not be after the '
                    'last day of the current month.')

            return string

        def valid_token(string):
            string = str(string)
            if not re.match('[a-f0-9]{32}', string):
                raise argparse.ArgumentTypeError('Invalid API token.')
            return string

        parser = argparse.ArgumentParser()
        parser.add_argument('-t', '--token', type=valid_token, required=True,
                            metavar='0123456789abcdef0123456789abcdef',
                            help='A valid Pivotal Tracker API token.')
        parser.add_argument('-p', '--project-id', type=int, required=True,
                            metavar='12345',
                            help='A valid Pivotal Tracker project ID.')
        parser.add_argument('-l', '--label', action='append', required=False,
                            metavar='label',
                            help='The label to be used to fetched the stories. '
                                 'Can be used multiple times.')
        parser.add_argument('-ua', '--updated-after', type=valid_date,
                            required=False,
                            metavar='YYYY-MM-DD',
                            help='Finds all stories that were last updated '
                                 'after the given date. The date format is '
                                 'YYYY-MM-DD.')
        parser.add_argument('-ub', '--updated-before', type=valid_date,
                            required=False,
                            metavar='YYYY-MM-DD',
                            help='Finds all stories that were last updated '
                                 'before the given date. The date format is '
                                 'YYYY-MM-DD.')
        parser.add_argument('-f', '--fields', type=str,
                            metavar='field1,field2,field3,...',
                            help='Comma-separated output fields. The fields are'
                                 ' the headers in the output.')
        parser.add_argument('-o', '--output-format', type=str,
                            choices=['table', 'json', 'yaml', 'csv'],
                            default='table',
                            help='The output format to be used.')
        parser.add_argument('-w', '--write-to-file', type=str,
                            metavar='filename',
                            help='Write to the given filename.')
        return parser

    def __filter_output_fields(self, stories, fields):
        """
        :param fields: A comma-separated list of strings.
        :return: filtered output.
        """
        fields_list = [field.strip() for field in fields.split(',')]

        filtered_stories = list()
        for story in stories:
            s = dict()
            for field in fields_list:
                try:
                    s[field] = story[field]
                except KeyError as e:
                    print('Invalid field name \'%s\'.' % field)
                    sys.exit(1)
            filtered_stories.append(s)
        return filtered_stories


    def process_commands(self):
        if '-t' not in sys.argv and '--token' not in sys.argv and \
                        'TOKEN' in os.environ:
            sys.argv.insert(1, os.environ['TOKEN'])
            sys.argv.insert(1, '--token')

        if '-p' not in sys.argv and '--project-id' not in sys.argv and \
                        'PROJECT_ID' in os.environ:
            sys.argv.insert(1, os.environ['PROJECT_ID'])
            sys.argv.insert(1, '--project-id')

        parser = self._get_parser()
        args = parser.parse_args()
        kwargs = dict()

        if args.label:
            kwargs[PTC.LABELS] = args.label
        if args.updated_after:
            kwargs[PTC.UPDATED_AFTER] = args.updated_after
        if args.updated_before:
            kwargs[PTC.UPDATED_BEFORE] = args.updated_before

        # fetch stories.
        fetcher = PivotalTrackerStoriesFetcher(
            token=args.token, project_id=args.project_id)
        stories = fetcher.fetch_stories(**kwargs)

        if args.fields:
            stories = self.__filter_output_fields(stories, args.fields)

        if args.output_format == 'csv':
            temp_fd, temp_fn = tempfile.mkstemp()
            with open(temp_fn, "w") as fp:
                csv_file = csv.writer(fp)
                headers = list(stories[0].keys())
                csv_file.writerow(headers)
                for story in stories:
                    csv_file.writerow([story[header] for header in headers])
            with open(temp_fn) as fp:
                formatted_output = fp.read()
            os.remove(temp_fn)
        elif args.output_format == 'json':
            formatted_output = json.dumps(stories, indent=2)
        elif args.output_format == 'yaml':
            formatted_output = yaml_dump(stories, allow_unicode=True)
        else:  #  args.output_format == 'table'
            headers = list(stories[0].keys())
            body = list()
            for story in stories:
                body.append([story[header] for header in headers])
            formatted_output = tabulate(
                body, headers=headers, tablefmt='pretty')

        if args.write_to_file:
            with open(args.write_to_file, 'w') as fp:
                fp.write(formatted_output)
        else:
            print(formatted_output.strip())


def main():
    return CommandProcessor().process_commands()


if __name__ == '__main__':
    sys.exit(main())
