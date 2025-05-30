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
from urllib.request import urlopen, Request
from datetime import datetime
from urllib import request, error
import xml.etree.ElementTree as ET
import re

TEMP_DIR = "/tmp"
HADOOP_CONF_DIR = "/etc/hadoop"
YARN_SITE_XML = "yarn-site.xml"
MAPRED_SITE_XML = "mapred-site.xml"
RM_ADDRESS_PROPERTY_NAME = "yarn.resourcemanager.webapp.address"
JHS_ADDRESS_PROPERTY_NAME = "mapreduce.jobhistory.webapp.address"

RM_LOG_REGEX = r"(?<=\")\/logs.+?RESOURCEMANAGER.+?(?=\")"
NM_LOG_REGEX = r"(?<=\")\/logs.+?NODEMANAGER.+?(?=\")"
INPUT_TIME_FORMAT = '%a %b %d %H:%M:%S %Z %Y'  # e.g. Wed May 28 07:35:39 UTC 2025
OUTPUT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S,%f'    # e.g. 2025-05-28 11:57:05,435


def application_failed():
    """
    Application Logs
    ResourceManager logs during job duration
    NodeManager logs from NodeManager where failed containers of jobs run during the duration of containers
    Job Configuration from MapReduce HistoryServer, Spark HistoryServer, TezHistory URL
    Job Related Metrics like Container, Attempts.
    """
    if args.arguments is None or len(args.arguments) == 0:
        print("Missing application or job id, exiting...")
        sys.exit(os.EX_USAGE)

    id = args.arguments[0]

    if "job" in id:
        output_path = create_output_dir(os.path.join(TEMP_DIR, id))

        # Get job log
        command = run_cmd_and_save_output(os.path.join(output_path, "job_logs"), id, "mapred", "job", "-logs",
                              id)  # TODO user permission?

        # Get job status
        job_status_string = run_command("mapred", "job", "-status", id)
        write_output(output_path, "job_status", job_status_string)

        # Finding JHS when running Hadoop with Hadock
        jhs_match = re.search(r'Job Tracking URL\s*:\s*http://([a-zA-Z0-9._-]+:\d+)', job_status_string)
        if jhs_match:
            JHS_ADDRESS = jhs_match.group(1)
            print("Job History Server Address: ", JHS_ADDRESS)

        # Get job attempts
        job_attempts_string = create_request("http://{}/ws/v1/history/mapreduce/jobs/{}/jobattempts"
                                                             .format(JHS_ADDRESS, id))
        write_output(output_path, "job_attempts", job_attempts_string)

        # Get job counters
        job_counters_string = create_request("http://{}/ws/v1/history/mapreduce/jobs/{}/counters"
                                                              .format(JHS_ADDRESS, id))
        write_output(output_path, "job_counters", job_counters_string)

        # Get job conf
        job_conf = create_request("http://{}/jobhistory/job/{}/conf"
                                                  .format(JHS_ADDRESS, id), False)
        write_output(os.path.join(output_path, "conf"), "job_conf.html", job_conf)

        # Get job start_time and end_time
        start_time, end_time = get_job_time(job_conf)
        print("Job start time: {}, end time: {}".format(start_time, end_time))

        # TODO Spark HistoryServer/TezHistory URL?

        # Get RM log
        log_address = get_node_log_address(RM_ADDRESS, RM_LOG_REGEX)
        write_output(os.path.join(output_path, "node_log"), "resourcemanager_log",
                     filter_node_log(log_address, start_time, end_time))
        # TODO filter RM logs for the run duration

        # Get NodeManager logs in the duration of containers belonging to app_id
        if "nodeHttpAddress" in job_attempts_string:
            job_attempts = ET.fromstring(job_attempts_string)
            nm_address = job_attempts.find(".//nodeHttpAddress").text
            log_address = get_node_log_address(nm_address, NM_LOG_REGEX)
            write_output(os.path.join(output_path, "node_log"), "nodemanager_log", get_container_log(log_address, id))

        command.communicate()
        return output_path
    elif "app" in id:
        output_path = create_output_dir(os.path.join(TEMP_DIR, id))

        # Get application info
        app_info_string = create_request("http://{}/ws/v1/cluster/apps/{}"
                                                         .format(RM_ADDRESS, id))
        write_output(output_path, "application_info", app_info_string)

        # Get application attempts
        app_attempts = create_request("http://{}/ws/v1/cluster/apps/{}/appattempts"
                                                      .format(RM_ADDRESS, id))
        write_output(output_path, "application_attempts", app_attempts)

        # Get start_time and end_time of the application
        start_time, end_time = get_application_time(app_info_string)

        # Get RM log
        log_address = get_node_log_address(RM_ADDRESS, RM_LOG_REGEX)
        write_output(os.path.join(output_path, "node_log"), "resourcemanager_log",
                     filter_node_log(log_address, start_time, end_time))

        # Get NodeManager logs in the duration of containers belonging to app_id
        if "amHostHttpAddress" in app_info_string:
            app_info = ET.fromstring(app_info_string)
            nm_address = app_info.find("amHostHttpAddress").text
            log_address = get_node_log_address(nm_address, NM_LOG_REGEX)
            write_output(os.path.join(output_path, "node_log"), "nodemanager_log", get_container_log(log_address, id))

        # Get application log
        command = run_cmd_and_save_output(os.path.join(output_path, "app_logs"), id, "yarn", "logs", "-applicationId",
                              id)  # TODO user permission?

        command.communicate()
        return output_path
    else:
        "Invalid application or job id."
        sys.exit(os.EX_USAGE)


def application_hanging():
    """
        Application Logs, Application Info, Application Attempts
        For Mapreduce: job configuration
        Hanging Container info: container_id, node_address, attempt_state and logs link
        Multiple JStack of Hanging Containers and NodeManager
        ResourceManager logs, NodeManager logs during job duration.
        NodeManager logs from NodeManager where hanging containers of jobs run during the duration of containers.
    """
    app_id = args.arguments[0]

    output_path = create_output_dir(os.path.join(TEMP_DIR, app_id))
    # TODO: http://nm-http-address:port/ws/v1/node/apps/{appid}

    # Get JStack of the hanging containers
    nm_address = get_nodemanager_address(app_id)
    app_jstack = create_request("http://{}/ws/v1/node/apps/{}/jstack".format(nm_address, app_id), False)
    write_output(output_path, "application_jstack", app_jstack)

    # Get JStack of the hanging NodeManager
    nm_jstack = create_request("http://{}/ws/v1/node/jstack".format(nm_address), False)
    write_output(output_path, "nm_{}_jstack".format(nm_address), nm_jstack)

    # Get application info
    app_info= create_request("http://{}/ws/v1/cluster/apps/{}".format(RM_ADDRESS, app_id))
    write_output(output_path, "application_info", app_info)

    # Get application attempts
    app_attempts = create_request("http://{}/ws/v1/cluster/apps/{}/appattempts".format(RM_ADDRESS, app_id))
    write_output(output_path, "application_attempts", app_attempts)

    # Get start_time and end_time of the application
    start_time, end_time = get_application_time(app_info)

    # Get RM log
    log_address = get_node_log_address(RM_ADDRESS, RM_LOG_REGEX)
    write_output(os.path.join(output_path, "node_log"), "resourcemanager_log",
                 filter_node_log(log_address, start_time, end_time))

    # Get NodeManager logs in the duration of containers belonging to app_id
    if "amHostHttpAddress" in app_info:
        log_address = get_node_log_address(nm_address, NM_LOG_REGEX)
        write_output(os.path.join(output_path, "node_log"), "nodemanager_log", get_container_log(log_address, app_id))

    # Get application log, may take a long time
    command = run_cmd_and_save_output(os.path.join(output_path, "app_logs"), app_id, "yarn", "logs", "-applicationId",
                                      app_id)  # TODO user permission?

    command.communicate()

    return output_path


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
    node_info_string = create_request("http://{}/ws/v1/cluster/nodes/{}"
                                                      .format(RM_ADDRESS, node_id))
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


####################################################### Utils Functions ###############################################


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

def run_command(*argv):
    try:
        cmd = " ".join(arg for arg in argv)
        print("Running command with arguments:", cmd)
        response = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, check=True)
        response_str = response.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        response_str = "Command failed with error: {}".format(str(e))
        print("Unable to run command: ", response_str)
    except Exception as e:
        response_str = "Exception occurred: {}".format(str(e))
        print("Exception occurred: ", response_str)

    return response_str

def run_cmd_and_save_output(output_path, out_filename, *argv):
    file_path = os.path.join(create_output_dir(output_path), out_filename)
    with open(file_path, 'w') as f:
        return subprocess.Popen(argv, stdout=f)


def create_request(url, xml_type=True):
    headers = {}
    # TODO auth can be handled here
    if xml_type:
        headers["Accept"] = "application/xml"

    try:
        req = request.Request(url, headers=headers)
        response = request.urlopen(req)
        response_str = response.read().decode('utf-8')
    except error.HTTPError as e:
        response_str = "HTTP error occurred: {} - {}".format(e.code, e.reason)
        print("Request failed: ", response_str)
    except Exception as e:
        response_str = "Unexpected error: {}".format(e)
        print("Request failed: {}".format(response_str))

    return response_str

def get_nodemanager_address(app_id):
    app_info = create_request("http://{}/ws/v1/cluster/apps/{}".format(RM_ADDRESS, app_id))
    app_info_xml = ET.fromstring(app_info)
    return app_info_xml.find("amHostHttpAddress").text


def get_node_log_address(node_address, link_regex):
    try:
        log_page = create_request("http://{}/logs/".format(node_address), False)
        matches = re.findall(link_regex, log_page, re.MULTILINE)
        if not matches:
            return "Warning: No matching log links found at {}/logs/".format(node_address)
        return node_address + matches[0]
    except Exception as e:
        return "Failed to retrieve node logs address from {}: {}".format(node_address, e)


def filter_node_log(node_log_address, start_time, end_time):
    return run_command("curl", "-s", "http://{}".format(node_log_address), "|", "sed", "-n",
                       "'/{}/,/{}/p'".format(start_time, end_time))


def get_container_log(log_address, id):
    return run_command("curl", "http://{}".format(log_address), "|", "grep", re.sub(r"^(job|application)", "container", id))


def get_application_time(app_info_string):
    app_element = ET.fromstring(app_info_string)
    start_time_epoch = int(app_element.find("startedTime").text)
    finish_time_epoch = int(app_element.find("finishedTime").text)

    start_time_str = datetime.fromtimestamp(start_time_epoch / 1000).strftime(OUTPUT_TIME_FORMAT)[:-4]  # -4, the time conversion is not accurrate
    finish_time_str = datetime.fromtimestamp(finish_time_epoch / 1000).strftime(OUTPUT_TIME_FORMAT)[:-4]

    return start_time_str, finish_time_str


def get_job_time(job_conf):
    times = re.findall(r'<th>\s*(Started|Finished):\s*</th>\s*<td>\s*(.*?)\s*</td>', job_conf)
    print("Job time: ", times)

    formatted_times = []

    for _, time in times:
        formatted_time = datetime.strptime(time, INPUT_TIME_FORMAT).strftime(OUTPUT_TIME_FORMAT)[:-7]  # -7 to omit milliseconds
        formatted_times.append(formatted_time)

    return formatted_times




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
    sys.exit(1)

JHS_ADDRESS = parse_url_from_conf(MAPRED_SITE_XML, JHS_ADDRESS_PROPERTY_NAME)
if JHS_ADDRESS is None:
    print("JHS address can't be found, exiting...")
    sys.exit(1)

func = ISSUE_MAP[args.command]
print(func())
