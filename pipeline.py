# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import os.path
import random
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import ExternalProcess
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
                           UploadWithTracker, SendDoneToTracker
import shutil
import socket
import subprocess
import sys
import time
import string

from tornado import httpclient

import seesaw
#from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable

# TODO
import json
import warcio
import requests

# check the seesaw version
if StrictVersion(seesaw.__version__) < StrictVersion('0.10.3'):
    raise Exception('This pipeline needs seesaw version 0.10.3 or higher.')


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_LUA will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string
# TODO
PYTHON = find_executable(
    'Wget+Lua',
    ['Python 2.7'],
    [
        '/usr/bin/python2',
        '/usr/local/bin/python2',
        './python2',
    ]
)

if not PYTHON:
    raise Exception('No usable Python 2 found.')


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = '20191022.00'
USER_AGENT = 'ArchiveTeam'
TRACKER_ID = 'yahoo-groups-py'
# TRACKER_HOST = 'tracker.archiveteam.org'  #prod-env
TRACKER_HOST = 'tracker-test.ddns.net'  #dev-env


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'CheckIP')
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 6:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class CheckBan(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'CheckBan')

    def process(self, item):
        msg = None
        httpclient.AsyncHTTPClient.configure(None, defaults=dict(user_agent=USER_AGENT))
        http_client = httpclient.HTTPClient()
        try:
            response = http_client.fetch("https://groups.yahoo.com/neo/search")  # dynamic
        except httpclient.HTTPError as e:
            msg = "Failed to get CheckBan URL: " + str(e)
            item.log_output(msg)
            item.log_output("Sleeping 60...")
            time.sleep(60)
        http_client.close()
        if msg != None:
            raise Exception(msg)


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, 'PrepareDirectories')
        self.warc_prefix = warc_prefix

    def process(self, item):
        start_time = time.strftime('%Y%m%d-%H%M%S')

        item_name = item['item_name']
        escaped_item_name = item_name.replace(':', '_').replace('/', '_').replace('~', '_')
        dirname = '/'.join((item['data_dir'], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item['item_dir'] = dirname
        item['start_time'] = start_time
        item['warc_file_base'] = '%s-%s-%s'    % (self.warc_prefix, escaped_item_name[:50],      start_time)
        item['warc_new_base']  = '%s-%s.%s-%s' % (self.warc_prefix, escaped_item_name[:50], '|', start_time)

        open('%(item_dir)s/%(warc_file_base)s.warc.gz' % item, 'w').close()
        open('%(item_dir)s/%(warc_file_base)s.defer-urls.txt' % item, 'w').close()


class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'MoveFiles')

    def process(self, item):
        # NEW for 2014! Check if wget was compiled with zlib support
        if os.path.exists('%(item_dir)s/%(warc_file_base)s.warc' % item):
            raise Exception('Please compile wget with zlib support!')

        item['warc_new_base'] = item['warc_new_base'].replace("|", str(item['version']))
        os.rename('%(item_dir)s/%(warc_file_base)s.warc.gz' % item,
                  '%(data_dir)s/%(warc_file_base)s.warc.gz' % item)
        os.rename('%(item_dir)s/%(warc_file_base)s.defer-urls.txt' % item,
                  '%(data_dir)s/%(warc_file_base)s.defer-urls.txt' % item)

        shutil.rmtree('%(item_dir)s' % item)
        item['files']=[ ItemInterpolation('%(data_dir)s/%(warc_file_base)s.defer-urls.txt') ]
        if item['todo_url_count'] != '0':
            item['files'].append( ItemInterpolation('%(data_dir)s/%(warc_file_base)s.warc.gz') )

def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()


CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
YP_SHA1 = get_hash(os.path.join(CWD, 'yahoo.py'))
YGAP_SHA1 = get_hash(os.path.join(CWD, 'yahoogroupsapi.py'))


def stats_id_function(item):
    # NEW for 2014! Some accountability hashes and stats.
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'yp_hash': YP_SHA1,
        'ygap_hash': YGAP_SHA1,
        'python_version': sys.version,
    }

    return d


class YgaArgs(object):
    def realize(self, item):
        yga_args = [
            PYTHON,
            'yahoo.py',
            '-a',
            '-e'
            #'-U', USER_AGENT,
            #'-nv',
            #'--no-cookies',
            #'--lua-script', 'yourshot-static.lua',
            #'-o', ItemInterpolation('%(item_dir)s/wget.log'),
            #'--no-check-certificate',
            #'--output-document', ItemInterpolation('%(item_dir)s/wget.tmp'),
            #'--truncate-output',
            #'-e', 'robots=off',
            #'--rotate-dns',
            # '--recursive', '--level=inf',
            # '--no-parent',
            # '--page-requisites',
            #'--timeout', '30',
            #'--tries', 'inf',
            # '--domains', 'nationalgeographic.com',
            # '--span-hosts',
            #'--waitretry', '30',
            #'--warc-file', ItemInterpolation('%(item_dir)s/%(warc_file_base)s'),
            #'--warc-header', 'operator: Archive Team',
            #'--warc-header', 'yourshot-static-dld-script-version: ' + VERSION,
            #'--warc-header', ItemInterpolation('yourshot-static-item: %(item_name)s'),
            # --warc-header yourshot-photo-id: ... filled in below
            # '--header', 'Accept-Encoding: gzip',
            # '--compression', 'gzip'
            # changed flags #
        ]

        item_name = item['item_name']
        assert ':' in item_name
        item_type, item_value = item_name.split(':', 1)

        item['item_type'] = item_type
        item['item_value'] = item_value

        httpclient.AsyncHTTPClient.configure(None, defaults=dict(user_agent=USER_AGENT))
        http_client = httpclient.HTTPClient()

        if item_type.startswith('ys_static_'):
            wget_urls = []
            defer_assets = []
            photo_ids = []
            item_version = None

            item_type_dir = item_type.split('_', 3)[2]
            job_file_url = ('https://raw.githubusercontent.com/marked/yourshot-static-items/master/'
                            + item_type_dir + '/' + item_value)  #prod-env | #dev-env

            print("Job location: " + job_file_url)  #debug
            job_file_resp = http_client.fetch(job_file_url, method='GET')  # url to github
            for task_line in job_file_resp.body.decode('utf-8', 'ignore').splitlines():
                task_line = task_line.strip()
                if len(task_line) == 0:
                    continue
                if item_type == 'ys_static_json':
                    print("Tv  " + task_line)  #debug
                    task_line_resp = http_client.fetch(task_line, method='GET')  # url to ys json api
                    api_resp = json.loads(task_line_resp.body.decode('utf-8', 'ignore'))
                    for photo_obj in api_resp["results"]:
                        wget_args.extend(['--warc-header',
                                          'yourshot-photo-id: {}'.format(photo_obj["photo_id"])])
                        for photo_size in photo_obj["thumbnails"]:
                            wget_urls.append("https://yourshot.nationalgeographic.com"
                                             + photo_obj["thumbnails"][photo_size])
                        defer_assets.append(photo_obj["detail_url"])
                        defer_assets.append(photo_obj["owner"]["profile_url"])
                        defer_assets.append(photo_obj["owner"]["avatar_url"])

                    print("\nIDs: {}/{}".format(len(api_resp["results"]), api_resp["count"]))  #debug
                    item_version = api_resp['count']

                    with open('%(item_dir)s/%(warc_file_base)s.defer-urls.txt' % item, 'w') as fh:
                        fh.write("IDs: {}/{}\n".format(len(api_resp["results"]), api_resp["count"]))
                        fh.writelines("%s\n" % asset for asset in defer_assets)
                elif item_type == 'ys_static_urls':
                    print("T>  " + task_line)  #debug
                    wget_urls.append(task_line)

            if item_version is None:
                item_version = len(wget_urls)
            item["version"] = item_version
            item["todo_url_count"] = str(len(wget_urls))

            print("URIs ToDo: {}".format(len(wget_urls)))
            if len(wget_urls) == 0:
                wget_args.append("-V")
            else:
                wget_args.extend(wget_urls)

            # print("\nD^      ", end="")  #debug
            # print("\nD^      ".join(defer_assets))  #debug
            http_client.close()
        else:
            raise Exception('Unknown item')

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)


class YgaDownload(ExternalProcess):
    '''Download with process runner.'''
    def __init__(self, args, max_tries=1, accept_on_exit_code=None,
                 retry_on_exit_code=None, env=None, stdin_data_function=None):
        ExternalProcess.__init__(
            self, "YgaDownload",
            args=args, max_tries=max_tries,
            accept_on_exit_code=(accept_on_exit_code
                                 if accept_on_exit_code is not None else [0]),
            retry_on_exit_code=retry_on_exit_code,
            env=env)
        self.stdin_data_function = stdin_data_function

    def stdin_data(self, item):
        if self.stdin_data_function:
            return self.stdin_data_function(item)
        else:
            return b""



###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title='yahoo-groups-python',
    project_html='''
<img class="project-logo" alt="logo" src="https://www.archiveteam.org/images/7/7a/Yourshot-logo.png" height="50px"/>
<h2>https://yourshot.nationalgeographic.com
 <span class="links">
  <a href="https://yourshot.nationalgeographic.com/">Website</a>
  &middot;
  <a href="http://tracker.archiveteam.org/yourshot-static/">Leaderboard</a>
 </span>
</h2>
    '''
)  # TODO

pipeline = Pipeline(
    CheckIP(),
    CheckBan(),
    GetItemFromTracker('http://%s/%s' % (TRACKER_HOST, TRACKER_ID), downloader, VERSION),
    PrepareDirectories(warc_prefix='yga-py'),
    YgaDownload(
        YgaArgs(),
        max_tries=0,              # 2,          #changed
        accept_on_exit_code=[0],  # [0, 4, 8],  #changed
        env={
            'item_dir': ItemValue('item_dir'),
            'item_value': ItemValue('item_value'),
            'item_type': ItemValue('item_type'),
            'warc_file_base': ItemValue('warc_file_base'),
            'todo_url_count': ItemValue('todo_url_count'),
        }
    ),
    PrepareStatsForTracker(
        defaults={'downloader': downloader, 'version': VERSION},
        file_groups={
            'data': [
                ItemInterpolation('%(item_dir)s/%(warc_file_base)s.warc.gz')  #TODO ?
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=20, default='20',
                                      name='shared:rsync_threads', title='Rsync threads',
                                      description='The maximum number of concurrent uploads.'),
                    UploadWithTracker('http://%s/%s' % (TRACKER_HOST, TRACKER_ID),
                                      downloader=downloader,
                                      version=VERSION,
                                      files=ItemValue('files'),
                                      rsync_target_source_path=ItemInterpolation('%(data_dir)s/'),
                                      rsync_extra_args=[
                                                         '--recursive',
                                                         '--partial',
                                                         '--partial-dir', '.rsync-tmp',
                                                       ]),),
    SendDoneToTracker(
        tracker_url='http://%s/%s' % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue('stats')
    )
)
