import argparse
from copy import deepcopy
from datetime import datetime as dt
import json
import os
import re
import requests
import sys


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
                story[PTC.CYCLE_TIME_DETAILS][PTC.CYCLE_TIME] = str(
                    dt.fromisoformat(acceptances[-1][PTC.OCCURRED_AT][:-1]) -
                    dt.fromisoformat(starts[-1][PTC.OCCURRED_AT][:-1]))
            story[PTC.CYCLE_TIME_DETAILS].pop(PTC.TOTAL_CYCLE_TIME, None)
            story[PTC.CYCLE_TIME_DETAILS].pop(PTC.STARTED_TIME, None)
            story[PTC.CYCLE_TIME_DETAILS].pop(PTC.FINISHED_TIME, None)
            story[PTC.CYCLE_TIME_DETAILS].pop(PTC.DELIVERED_TIME, None)
            story[PTC.CYCLE_TIME_DETAILS].pop(PTC.KIND, None)
            story[PTC.CYCLE_TIME_DETAILS].pop(PTC.STORY_ID, None)
            story[PTC.CYCLE_TIME_DETAILS].pop('', None)
            story[PTC.CYCLE_TIME_DETAILS].pop('', None)
            story[PTC.CYCLE_TIME_DETAILS].pop('', None)
        return pruned_stories

    def fetch_stories(
            self, labels=None, updated_after=None, updated_before=None,
            fields=None, prune=True):
        default_fields = \
            [PTC.ACCEPTED_AT, PTC.CURRENT_STATE, PTC.CYCLE_TIME_DETAILS,
             PTC.ESTIMATE, PTC.ID, PTC.LABELS, PTC.NAME, PTC.OWNER_IDS,
             PTC.STORY_TYPE, PTC.TRANSITIONS, PTC.URL]

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
                            help='A valid Pivotal Tracker API token.')
        parser.add_argument('-p', '--project-id', type=int, required=True,
                            help='A valid Pivotal Tracker project ID.')
        parser.add_argument('-l', '--labels', action='append', required=False,
                            help='The label to be used to fetched the stories.')
        parser.add_argument('-ua', '--updated-after', type=valid_date,
                            required=False,
                            help='Finds all stories that were last updated '
                                 'after the given date. The date format is '
                                 'YYYY-MM-DD.')
        parser.add_argument('-ub', '--updated-before', type=valid_date,
                            required=False,
                            help='Finds all stories that were last updated '
                                 'before the given date. The date format is '
                                 'YYYY-MM-DD.')
        return parser

    def process_commands(self):
        if '-t' not in sys.argv and '--token' not in sys.argv and \
                        'TOKEN' in os.environ:
            sys.argv.insert(1, os.environ.get('TOKEN', ''))
            sys.argv.insert(1, '--token')

        if '-p' not in sys.argv and '--project-id' not in sys.argv and \
                        'PROJECT_ID' in os.environ:
            sys.argv.insert(1, os.environ.get('PROJECT_ID', ''))
            sys.argv.insert(1, '--project-id')

        parser = self._get_parser()
        args = parser.parse_args()
        kwargs = dict()

        if args.labels:
            kwargs[PTC.LABELS] = args.labels
        if args.updated_after:
            kwargs[PTC.UPDATED_AFTER] = args.updated_after
        if args.updated_before:
            kwargs[PTC.UPDATED_BEFORE] = args.updated_before

        # fetch stories.
        fetcher = PivotalTrackerStoriesFetcher(
            token=args.token, project_id=args.project_id)
        stories = fetcher.fetch_stories(**kwargs)

        print(json.dumps(stories, indent=2))


def main():
    return CommandProcessor().process_commands()


if __name__ == '__main__':
    sys.exit(main())
