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


def application_failed():
    # Prints out out dir correctly
    print("application_failed")
    print("out_dir:/tmp")


def application_failed_necessary_args():
    # Prints out out dir correctly only if "arg1" and "arg2" are present
    if args.arguments is None or len(args.arguments) is not 2:
        sys.exit(os.EX_USAGE)
    elif args.arguments[0] == "appId" and args.arguments[1] == "nodeId":
        print("out_dir:/tmp")


def application_hanging():
    # Prints out empty out dir
    print("application_hanging")
    print("out_dir:")


def scheduler_related_issue():
    # Doesn't print out out dir
    print("scheduler_related_issue")


def rm_nm_start_failure():
    sys.exit(os.EX_OK)


def list_issues():
    print("application_failed:appId", "application_hanging:appId,nodeId", "scheduler_related_issue",
          "rm_nm_start_failure:nodeId", "rm_nm_start_failure:nodeId", "rm_nm_start_failure_1:nodeId:nodeId", sep="\n")


ISSUE_MAP = {
    "application_failed": application_failed,
    "application_failed_necessary_args": application_failed_necessary_args,
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

func = ISSUE_MAP[args.command]
func()