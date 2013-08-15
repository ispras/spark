#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
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
#

from sys import stderr

# Get number of local disks available for a given Openstack instance type.
def get_num_disks(instance_type):
    disks_by_instance = {
        "o1.large": 1
    }
    if instance_type in disks_by_instance:
        return disks_by_instance[instance_type]
    else:
        print >> stderr, ("WARNING: Don't know number of disks on instance type %s; assuming 1"
                          % instance_type)
        return 1
