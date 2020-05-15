from datetime import datetime
import json
import requests

TOKEN = 'NO_YOU_DON\'T_SEE_THIS'  # Secret token. Don't hack.
PROJECT_ID = '123456789'

labels = ['label1', 'sprint 1', 'p1']

STORIES_URL = 'https://www.pivotaltracker.com/services/v5/projects/{PROJECT_ID}/stories'
STORY_TRANSITIONS_URL = 'https://www.pivotaltracker.com/services/v5/projects/{PROJECT_ID}/stories/{STORY_ID}/transitions'
MEMBERSHIPS_URL = 'https://www.pivotaltracker.com/services/v5/projects/{PROJECT_ID}/memberships'

headers = {'X-TrackerToken': TOKEN}
params = {'filter': 'label:"{label}"'.format(label=labels[0]), 'fields': 'id,accepted_at,estimate,story_type,name,current_state,url,owner_ids,estimate,transitions,cycle_time_details,labels'}

response = requests.get(MEMBERSHIPS_URL.format(PROJECT_ID=PROJECT_ID), headers=headers)
users = [membership['person'] for membership in json.loads(response.text)]

response = requests.get(STORIES_URL.format(PROJECT_ID=PROJECT_ID), params=params, headers=headers)
stories = json.loads(response.text)

if len(labels) > 1:
    for label in labels[1:]:
        filtered_stories = list()
        for story in stories:
            story_labels = [l['name'] for l in story.get('labels', [])]
            if label not in story_labels:
                continue
            filtered_stories.append(story)
        stories = filtered_stories

def get_dict_from_list(list_of_dict, key, value):
    return list(filter(
        lambda dictionary: dictionary[key] == value, list_of_dict))

def prune_stories(stories, users):
    for story in stories:
        story['labels'] = [label['name'] for label in story.pop('labels')]
        story['owners'] = [get_dict_from_list(users, 'id', owner_id)[0]['name'] for owner_id in story.pop('owner_ids')]
        for transition in story['transitions']:
            transition['performed_by'] = get_dict_from_list(users, 'id', transition.pop('performed_by_id'))[0]['name']
            transition.pop('story_id')
            transition.pop('project_id')
            transition.pop('project_version')
            transition.pop('kind')
        story['transitions'] = sorted(story.pop('transitions'), key=lambda k: datetime.fromisoformat(k['occurred_at'][:-1]))

        starts = get_dict_from_list(story['transitions'], 'state', 'started')
        acceptances = get_dict_from_list(story['transitions'], 'state', 'accepted')
        if starts and acceptances:
            story['cycle_time_details']['cycle_time'] = str(\
                datetime.fromisoformat(acceptances[-1]['occurred_at'][:-1]) - \
                    datetime.fromisoformat(starts[-1]['occurred_at'][:-1]))
        story['cycle_time_details'].pop('total_cycle_time', None)
        story['cycle_time_details'].pop('started_time', None)
        story['cycle_time_details'].pop('', None)
        story['cycle_time_details'].pop('', None)
        story['cycle_time_details'].pop('', None)
        story['cycle_time_details'].pop('', None)
        story['cycle_time_details'].pop('', None)
        story['cycle_time_details'].pop('', None)
        story['cycle_time_details'].pop('', None)



prune_stories(stories, users)


x = stories[0]
# Get transitions

# for story in stories:
