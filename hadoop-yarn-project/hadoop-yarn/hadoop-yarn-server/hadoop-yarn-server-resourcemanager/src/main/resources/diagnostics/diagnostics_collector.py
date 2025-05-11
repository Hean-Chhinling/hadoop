# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function

import argparse
import sys, os
import subprocess
import urllib2
import xml.etree.ElementTree as ET
import re

TEMP_DIR = "/tmp"
HADOOP_CONF_DIR = "/etc/hadoop/conf"
YARN_SITE_XML = "yarn-site.xml"
MAPRED_SITE_XML = "mapred-site.xml"
RM_ADDRESS_PROPERTY_NAME = "yarn.resourcemanager.webapp.address"
JHS_ADDRESS_PROPERTY_NAME = "mapreduce.jobhistory.webapp.address"

RM_LOG_REGEX = r"(?<=\")\/logs.+?RESOURCEMANAGER.+?(?=\")"
NM_LOG_REGEX = r"(?<=\")\/logs.+?NODEMANAGER.+?(?=\")"


def application_failed():
    """
    Application Logs
    ResourceManager logs during job duration
    NodeManager logs from NodeManager where failed containers of jobs run during the duration of containers
    Job Configuration from MapReduce HistoryServer, Spark HistoryServer, TezHistory URL
    Job Related Metrics like Container, Attempts.

    :return:
    """
    if args.arguments is None or len(args.arguments) is 0:
        print("Missing application or job id, exiting...")
        sys.exit(os.EX_USAGE)

    id = args.arguments[0]

    if "job" in id:
        output_path = create_output_dir(os.path.join(TEMP_DIR, id))

        # Get job log
        command = run_command(os.path.join(output_path, "job_logs"), id, "mapred", "job", "-logs",
                              id)  # TODO user permission?

        # Get job attempts
        job_attempts_string = urllib2.urlopen(create_request("http://{}/ws/v1/history/mapreduce/jobs/{}/jobattempts"
                                                             .format(JHS_ADDRESS, id))).read()
        write_output(output_path, "job_attempts", job_attempts_string)

        # Get job counters
        job_counters_string = urllib2.urlopen(create_request("http://{}/ws/v1/history/mapreduce/jobs/{}/counters"
                                                             .format(JHS_ADDRESS, id))).read()
        write_output(output_path, "job_counters", job_counters_string)

        # Get job conf
        job_conf = urllib2.urlopen(create_request("http://{}/jobhistory/job/{}/conf"
                                                  .format(JHS_ADDRESS, id), False)).read()
        write_output(os.path.join(output_path, "conf"), "job_conf.html", job_conf)

        # TODO Spark HistoryServer/TezHistory URL?

        # Get RM log
        write_output(os.path.join(output_path, "node_log"), "resourcemanager_log",
                     get_node_logs(RM_ADDRESS, RM_LOG_REGEX))
        # TODO filter RM logs for the run duration

        # Get NM log
        job_attempts = ET.fromstring(job_attempts_string)
        nm_address = job_attempts.find(".//nodeHttpAddress").text
        write_output(os.path.join(output_path, "node_log"), "nodemanager_log",
                     get_node_logs(nm_address, NM_LOG_REGEX))
        # TODO filter NM logs for the run duration

        command.communicate()
        return output_path
    elif "app" in id:
        output_path = create_output_dir(os.path.join(TEMP_DIR, id))

        # Get application log
        command = run_command(os.path.join(output_path, "app_logs"), id, "yarn", "logs", "-applicationId",
                              id)  # TODO user permission?

        # Get application info
        app_info_string = urllib2.urlopen(create_request("http://{}/ws/v1/cluster/apps/{}"
                                                         .format(RM_ADDRESS, id))).read()
        write_output(output_path, "application_info", app_info_string)

        # Get application attempts
        app_attempts = urllib2.urlopen(create_request("http://{}/ws/v1/cluster/apps/{}/appattempts"
                                                      .format(RM_ADDRESS, id))).read()
        write_output(output_path, "application_attempts", app_attempts)

        # Get RM log
        write_output(os.path.join(output_path, "node_log"), "resourcemanager_log",
                     get_node_logs(RM_ADDRESS, RM_LOG_REGEX))
        # TODO filter RM logs for the run duration

        # Get NM log
        app_info = ET.fromstring(app_info_string)
        nm_address = app_info.find("amHostHttpAddress").text
        write_output(os.path.join(output_path, "node_log"), "nodemanager_log",
                     get_node_logs(nm_address, NM_LOG_REGEX))
        # TODO filter NM logs for the run duration

        command.communicate()
        return output_path
    else:
        "Invalid application or job id."
        sys.exit(os.EX_USAGE)


def application_hanging():
    print("application_hanging")
    # TODO: http://nm-http-address:port/ws/v1/node/apps/{appid}


def scheduler_related_issue():
    print("scheduler_related_issue")


def rm_nm_start_failure():
    if args.arguments is None or len(args.arguments) is 0:
        print("Missing node id, exiting...")
        sys.exit(os.EX_USAGE)

    node_id = args.arguments[0]
    output_path = create_output_dir(os.path.join(TEMP_DIR, "node_failure_{}".format(node_id.split(":")[0])))

    print("http://{}/ws/v1/cluster/nodes/{}"
                         .format(RM_ADDRESS, node_id))
    # Get node info
    node_info_string = urllib2.urlopen(create_request("http://{}/ws/v1/cluster/nodes/{}"
                                                      .format(RM_ADDRESS, node_id))).read()
    write_output(output_path, "node_info", node_info_string)

    # Get RM log
    write_output(os.path.join(output_path, "node_log"), "resourcemanager_log",
                 get_node_logs(RM_ADDRESS, RM_LOG_REGEX))

    # Get NM log
    node_info = ET.fromstring(node_info_string)
    nm_address = node_info.find("nodeHTTPAddress").text.split(":")[0]
    write_output(os.path.join(output_path, "node_log"), "nodemanager_log",
                 get_node_logs(nm_address, NM_LOG_REGEX))

    # TODO YARN/Scheduler configuration

    return output_path


def list_issues():
    print("application_failed:appId", "application_hanging:appId", "scheduler_related_issue",
          "rm_nm_start_failure:nodeId", sep="\n")


def parse_url_from_conf(conf_file, url_property_name):
    root = ET.parse(os.path.join(HADOOP_CONF_DIR, conf_file))
    for prop in root.findall("property"):
        prop_name = prop.find("name").text
        if prop_name == url_property_name:
            return prop.find("value").text

    return None


def create_output_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path


def write_output(output_path, out_filename, value):
    output_path = create_output_dir(output_path)
    with open(os.path.join(output_path, out_filename), 'w') as f:
        f.write(value)


def run_command(output_path, out_filename, *argv):
    file_path = os.path.join(create_output_dir(output_path), out_filename)
    with open(file_path, 'w') as f:
        return subprocess.Popen(argv, stdout=f)


def create_request(url, xml_type=True):
    req = urllib2.Request(url)
    # TODO auth can be handled here
    if xml_type:
        req.add_header("Accept", "application/xml")
    return req


def get_node_logs(node_address, link_regex):
    log_page = urllib2.urlopen(create_request("http://{}/logs/".format(node_address), False)).read()
    matches = re.findall(link_regex, log_page, re.MULTILINE)
    return urllib2.urlopen(create_request("http://{}".format(node_address + matches[0]), False)).read()


ISSUE_MAP = {
    "application_failed": application_failed,
    "application_hanging": application_hanging,
    "scheduler_related_issue": scheduler_related_issue,
    "rm_nm_start_failure": rm_nm_start_failure
}

parser = argparse.ArgumentParser()
parser.add_argument("-l", "--list", help="List the available issue types.", action="store_true")
parser.add_argument("-c", "--command", choices=list(ISSUE_MAP), help="Initiate the diagnostic information collecton"
                                                                     "for diagnosing the selected issue type.")
parser.add_argument("-a", "--arguments", nargs='*', help="The required arguments for the selected issue type.")
args = parser.parse_args()

if not (args.list or args.command):
    parser.error('No action requested, use --list or --command')

if args.list:
    list_issues()
    sys.exit(os.EX_OK)

RM_ADDRESS = parse_url_from_conf(YARN_SITE_XML, RM_ADDRESS_PROPERTY_NAME)
if RM_ADDRESS is None:
    print("RM address can't be found, exiting...")
    sys.exit(os.EX_NOTFOUND)

JHS_ADDRESS = parse_url_from_conf(MAPRED_SITE_XML, JHS_ADDRESS_PROPERTY_NAME)
if JHS_ADDRESS is None:
    print("JHS address can't be found, exiting...")
    sys.exit(os.EX_NOTFOUND)

func = ISSUE_MAP[args.command]
print(func())
