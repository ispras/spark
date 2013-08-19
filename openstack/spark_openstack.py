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

from __future__ import with_statement

import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import urllib2
from optparse import OptionParser
from sys import stderr

from flavors import get_num_disks

#sys.path.append(os.path.join(os.path.dirname(__file__), 'third_party/boto-2.9.9'))

#import boto
#from boto.ec2.blockdevicemapping import BlockDeviceMapping, EBSBlockDeviceType
#from boto import ec2

from novaclient.v1_1 import client

# A static URL from which to figure out the latest Mesos EC2 AMI
LATEST_AMI_URL = "https://s3.amazonaws.com/mesos-images/ids/latest-spark-0.7"


# Configure and parse our command-line arguments
def parse_args():
    parser = OptionParser(usage="spark-openstack [options] <action> <cluster_name>"
                                + "\n\n<action> can be: launch, destroy, login, stop, start, get-master",
                          add_help_option=False)
    parser.add_option("-h", "--help", action="help",
                      help="Show this help message and exit")
    parser.add_option("-s", "--slaves", type="int", default=1,
                      help="Number of slaves to launch (default: 1)")
    parser.add_option("-w", "--wait", type="int", default=60,
                      help="Seconds to wait for nodes to start (default: 60)")
    parser.add_option("-k", "--key-pair",
                      help="Key pair to use on instances")
    parser.add_option("-i", "--identity-file",
                      help="SSH private key file to use for logging into instances")
    parser.add_option("-t", "--instance-type", default="o1.large",
                      help="Type of instance to launch (default: o1.large). " +
                           "WARNING: must be 64-bit; small instances won't work" +
                           "NOTE: Make sure that instance type exists in your Openstack")
    parser.add_option("-m", "--master-instance-type", default="",
                      help="Master instance type (leave empty for same as instance-type)")
#    parser.add_option("-r", "--region", default="",
#                      help="EC2 region zone to launch instances in")
#    parser.add_option("-z", "--zone", default="nova",
#                      help="Availability zone to launch instances in, or 'all' to spread " +
#                           "slaves across multiple (an additional $0.01/Gb for bandwidth" +
#                           "between zones applies)")
    parser.add_option("-a", "--image-id",
                      help="Openstack Image ID to use.")
    parser.add_option("-D", metavar="[ADDRESS:]PORT", dest="proxy_port",
                      help="Use SSH dynamic port forwarding to create a SOCKS proxy at " +
                           "the given local address (for use with login)")
    parser.add_option("--resume", action="store_true", default=False,
                      help="Resume installation on a previously launched cluster " +
                           "(for debugging)")
#    parser.add_option("--ebs-vol-size", metavar="SIZE", type="int", default=0,
#                      help="Attach a new EBS volume of size SIZE (in GB) to each node as " +
#                           "/vol. The volumes will be deleted when the instances terminate. " +
#                           "Only possible on EBS-backed AMIs.")
    parser.add_option("--swap", metavar="SWAP", type="int", default=1024,
                      help="Swap space to set up per node, in MB (default: 1024)")
#    parser.add_option("--spot-price", metavar="PRICE", type="float",
#                      help="If specified, launch slaves as spot instances with the given " +
#                           "maximum price (in dollars)")
    parser.add_option("--cluster-type", type="choice", metavar="TYPE",
                      choices=["mesos", "standalone"], default="standalone",
                      help="'mesos' for a Mesos cluster, 'standalone' for a standalone " +
                           "Spark cluster (default: standalone)")
    parser.add_option("--ganglia", action="store_true", default=True,
                      help="Setup Ganglia monitoring on cluster (default: on). NOTE: " +
                           "the Ganglia page will be publicly accessible")
    parser.add_option("--no-ganglia", action="store_false", dest="ganglia",
                      help="Disable Ganglia monitoring for the cluster")
    parser.add_option("-u", "--user", default="root",
                      help="The SSH user you want to connect as (default: root)")
    parser.add_option("--delete-groups", action="store_true", default=False,
                      help="When destroying a cluster, delete the security groups that were created")
    parser.add_option("-l", "--openstack-login",
                      help="Your login name in Openstack")
    parser.add_option("-p", "--openstack-password",
                      help="Your password for Openstack")
    parser.add_option("-T", "--openstack-tenant-name",
                      help="Your tenant name in Openstack")
    parser.add_option("-d", "--openstack-address",
                      help="Openstack auth url with port (e.g. http://10.10.10.121:5000/v2.0) ")
    parser.add_option("-n", "--openstack-network-communication-method", type="choice", metavar="NET",
                      choices=["fixed", "floating", "public-dns-name"], default="fixed",
                      help="""Openstack communication way with instances. Choose between 'fixed', 'floating' and
                      'public-dns-name'. If you don't understand what does it mean, look at README in this folder""")

    (opts, args) = parser.parse_args()
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    (action, cluster_name) = args
    if opts.identity_file == None and action in ['launch', 'login', 'start']:
        print >> stderr, ("ERROR: The -i or --identity-file argument is " +
                          "required for " + action)
        sys.exit(1)
    if opts.image_id == None and action in ['launch', 'login', 'start']:
        print >> stderr, ("ERROR: The -a or --image-id argument is " +
                          "required to " + action)
        sys.exit(1)

    if opts.openstack_login == None:
        print >> stderr, ("ERROR: The -l or --openstack-login argument is " +
                          "required for any action")
        sys.exit(1)
    if opts.openstack_password == None:
        print >> stderr, ("ERROR: The -p or --openstack-password argument is " +
                          "required for any action")
        sys.exit(1)
    if opts.openstack_tenant_name == None:
        print >> stderr, ("ERROR: The -t or --openstack-tenant-name argument is " +
                          "required for any action")
        sys.exit(1)
    if opts.openstack_address == None:
        print >> stderr, ("ERROR: The -d or --openstack-address argument is " +
                          "required for any action")
        sys.exit(1)
    if opts.cluster_type not in ["mesos", "standalone"] and action == "launch":
        print >> stderr, ("ERROR: Invalid cluster type: " + opts.cluster_type)
        sys.exit(1)

    return (opts, action, cluster_name)


# Get the Openstack security group of the given name, creating it if it doesn't exist
def get_or_make_group(conn, name):
    groups = conn.security_groups.list()
    group = [g for g in groups if g.name == name]
    if len(group) > 0:
        return group[0]
    else:
        print "Creating security group " + name
        group = conn.security_groups.create(name, "Spark Openstack " + name + " group")
        return group


# Openstack novaclient library can't update instance info, we need to do it manually
def update_instances(conn, nodes_list):
    new_nodes_list = []
    number_of_nodes = len(nodes_list)
    for node in nodes_list:
            new_nodes_list.append( conn.servers.get(node.id))
    if number_of_nodes != len(new_nodes_list):
        print >> stderr, ("Some nodes disapperared, emergency")
        sys.exit(1)
    return new_nodes_list


# Wait for a set of launched instances to exit the "pending" state
# (i.e. either to start running or to fail and be terminated)
def wait_for_instances(conn, instances):
    while True:
        instances = update_instances(conn, instances)
        if len([i for i in instances if i.status == 'ACTIVE']) > 0:
            time.sleep(5)
        else:
            return


# Check whether an Openstack instance is not terminated. Openstack has more states than Amazon, so this place
# needs double-check (I could have missed something).
# NOTE: I consider "rescue" state as not active.

def is_active(instance):
    return (instance.status in ['INITIAL', 'BUILD', 'ACTIVE', 'SHUTOFF', 'STOP' 'SUSPENDED', 'PAUSED', 'REBOOT'])


# Authorize another group to have access to port range for the given group
#
def authorize_group(conn, dst_group_id, protocols, from_port=1, to_port=65535, cidr=None, src_group_id=None):
    if cidr == None and src_group_id == None:
        print >> stderr, ("ERROR: This should never happen, assertion " +
                          "in function authorize_group %d" % dst_group_id)
        sys.exit(1)
    for protocol in protocols:
        if protocol != 'icmp':
            conn.security_group_rules.create(parent_group_id=dst_group_id,
                                             ip_protocol=protocol,
                                             from_port=from_port,
                                             to_port=to_port,
                                             cidr=cidr,
                                             group_id=src_group_id)
        else:
            conn.security_group_rules.create(parent_group_id=dst_group_id,
                                             ip_protocol=protocol,
                                             from_port=-1,
                                             to_port=-1,
                                             cidr=cidr,
                                             group_id=src_group_id)

# Launch a cluster of the given name, by setting up its security groups,
# and then starting new instances in them.
# Returns a tuple of EC2 reservation objects for the master, slave
# and zookeeper instances (in that order).
# Fails if there already instances running in the cluster's groups.
def launch_cluster(conn, opts, cluster_name):
    print "Setting up security groups..."
    master_group = get_or_make_group(conn, cluster_name + "-master")
    slave_group = get_or_make_group(conn, cluster_name + "-slaves")
    zoo_group = get_or_make_group(conn, cluster_name + "-zoo")
    # Group was just now created
    if master_group.rules == []:
        # authorize cluster groups to have full access to each other
        authorize_group(conn, master_group.id, protocols=['tcp', 'udp', 'icmp'], src_group_id=master_group.id)
        authorize_group(conn, master_group.id, protocols=['tcp', 'udp', 'icmp'], src_group_id=slave_group.id)
        authorize_group(conn, master_group.id, protocols=['tcp', 'udp', 'icmp'], src_group_id=zoo_group.id)

        # ssh
        authorize_group(conn, master_group.id, protocols=['tcp'], from_port=22, to_port=22, cidr='0.0.0.0/0')

        # web-servers
        authorize_group(conn, master_group.id, protocols=['tcp'], from_port=8080, to_port=8081, cidr='0.0.0.0/0')

        # ?
        authorize_group(conn, master_group.id, protocols=['tcp'], from_port=50030, to_port=50030, cidr='0.0.0.0/0')

        # ?
        authorize_group(conn, master_group.id, protocols=['tcp'], from_port=50070, to_port=50070, cidr='0.0.0.0/0')

        # hdfs dfs http address
        authorize_group(conn, master_group.id, protocols=['tcp'], from_port=60070, to_port=60070, cidr='0.0.0.0/0')

        if opts.cluster_type == "mesos":
            authorize_group(conn, master_group.id, protocols=['tcp'], from_port=38090, to_port=38090, cidr='0.0.0.0/0')
        if opts.ganglia:
            authorize_group(conn, master_group.id, protocols=['tcp'], from_port=5080, to_port=5080, cidr='0.0.0.0/0')

    if slave_group.rules == []: # Group was just now created
        authorize_group(conn, slave_group.id, protocols=['tcp', 'udp', 'icmp'], src_group_id=master_group.id)
        authorize_group(conn, slave_group.id, protocols=['tcp', 'udp', 'icmp'], src_group_id=slave_group.id)
        authorize_group(conn, slave_group.id, protocols=['tcp', 'udp', 'icmp'], src_group_id=zoo_group.id)

        authorize_group(conn, slave_group.id, protocols=['tcp'], from_port=22, to_port=22, cidr='0.0.0.0/0')
        authorize_group(conn, slave_group.id, protocols=['tcp'], from_port=8080, to_port=8081, cidr='0.0.0.0/0')
        authorize_group(conn, slave_group.id, protocols=['tcp'], from_port=50060, to_port=50060, cidr='0.0.0.0/0')
        authorize_group(conn, slave_group.id, protocols=['tcp'], from_port=50075, to_port=50075, cidr='0.0.0.0/0')
        authorize_group(conn, slave_group.id, protocols=['tcp'], from_port=60060, to_port=60060, cidr='0.0.0.0/0')
        authorize_group(conn, slave_group.id, protocols=['tcp'], from_port=60075, to_port=60075, cidr='0.0.0.0/0')

    if zoo_group.rules == []: # Group was just now created
        authorize_group(conn, zoo_group.id, protocols=['tcp', 'udp', 'icmp'], src_group_id=master_group.id)
        authorize_group(conn, zoo_group.id, protocols=['tcp', 'udp', 'icmp'], src_group_id=slave_group.id)
        authorize_group(conn, zoo_group.id, protocols=['tcp', 'udp', 'icmp'], src_group_id=zoo_group.id)

        authorize_group(conn, zoo_group.id, protocols=['tcp'], from_port=22, to_port=22, cidr='0.0.0.0/0')
        authorize_group(conn, zoo_group.id, protocols=['tcp'], from_port=2181, to_port=2181, cidr='0.0.0.0/0')
        authorize_group(conn, zoo_group.id, protocols=['tcp'], from_port=2888, to_port=2888, cidr='0.0.0.0/0')
        authorize_group(conn, zoo_group.id, protocols=['tcp'], from_port=3888, to_port=3888, cidr='0.0.0.0/0')

    # Check if instances are already running in our groups
    active_nodes = get_existing_cluster(conn, opts, cluster_name,
                                        die_on_error=False)
    if any(active_nodes):
        print >> stderr, ("ERROR: There are already instances running in " +
                          "group %s, %s or %s" % (master_group.name, slave_group.name, zoo_group.name))
        sys.exit(1)

    print "Launching instances..."

    try:
        # try to find image with specified ID
        image = conn.images.get(opts.image_id)
    except:
        print >> stderr, "Could not find specified image: " + opts.ami
        sys.exit(1)

    # Launch slaves first
    slave_nodes = []
    for slave_id in range (0, opts.slaves, 1):
        instance_name = cluster_name + "-slave-" + str(slave_id)
        flav_found = False
        for flav in conn.flavors.list():
            if flav.name == opts.instance_type:
                print ("Instance type detected: " + opts.instance_type)
                flavor = flav
                flav_found = True
        if not flav_found:
             print >> stderr, "Could not find specified instance type for slave: " + opts.instance_type
             sys.exit(1)

#TODO: need to implement floating IPs here.
#TODO: implement quotas check here.
        slave_nodes.append( conn.servers.create(name=instance_name,
                                        image=image,
                                        security_groups={slave_group.name},
                                        key_name=opts.key_pair,
                                        flavor=flavor,
                                        min_count=1,
                                        max_count=1))
    print "Launched %d slaves" % opts.slaves

    # Launch master

    master_type = opts.master_instance_type
    if master_type == "":
        master_type = opts.instance_type
    master_nodes = []
    instance_name = cluster_name + "-master"
    flav_found = False
    for flav in conn.flavors.list():
        if flav.name == master_type:
            print ("Instance type detected: " + opts.instance_type)
            flavor = flav
            flav_found = True
    if not flav_found:
         print >> stderr, "Could not find specified instance type for slave: " + opts.instance_type
         sys.exit(1)
    master_nodes.append(conn.servers.create(name=instance_name,
                                     image=image,
                                     security_groups={master_group.name},
                                     key_name=opts.key_pair,
                                     flavor=flavor,
                                     min_count=1,
                                     max_count=1))

    print "Launched master"

    zoo_nodes = []

    # Return all the instances
    return (master_nodes, slave_nodes, zoo_nodes)


# Get instances in an existing cluster if available.
# Returns a tuple of lists of instance objects for the masters,
# slaves and zookeeper nodes (in that order).
def get_existing_cluster(conn, opts, cluster_name, die_on_error=True):
    print "Searching for existing cluster " + cluster_name + "..."
    reservations = conn.servers.list()
    master_nodes = []
    slave_nodes = []
    zoo_nodes = []
    for res in reservations:
        active = is_active(res)
        if active:
            group_names = [g["name"] for g in res.security_groups]
            if cluster_name + "-master" in group_names:
                master_nodes.append(res)
            elif cluster_name + "-slaves" in group_names:
                slave_nodes.append(res)
            elif cluster_name + "-zoo" in group_names:
                zoo_nodes.append(res)
    if any((master_nodes, slave_nodes, zoo_nodes)):
        print ("Found %d master(s), %d slaves, %d ZooKeeper nodes" %
               (len(master_nodes), len(slave_nodes), len(zoo_nodes)))
    if (master_nodes != [] and slave_nodes != []) or not die_on_error:
        return (master_nodes, slave_nodes, zoo_nodes)
    else:
        if master_nodes == [] and slave_nodes != []:
            print "ERROR: Could not find master in group " + cluster_name + "-master"
        elif master_nodes != [] and slave_nodes == []:
            print "ERROR: Could not find slaves in group " + cluster_name + "-slaves"
        else:
            print "ERROR: Could not find any existing cluster"
        sys.exit(1)


# This function gives an address to communicate with nodes. There are 3 general ways of addressing in Openstack:
# 1) Fixed local IPs. Lots of installation use only this type due to lack of network resources. It this case
#    networking usually uses static routes inside openstack virtual network. It's not secure but effective if you
#    don't have networking skills
# 2) Floating IPs. The most common case. Floating IPs may be used to be bridged to outer networks, so instances have
#    two addresses binded to one network interface so outer users may use e.g. addresses 192.168.0.* to access VMs
#    and VMs at the same time think that they have addresses 10.10.10.*
# 3) Public DNS names. The most difficult way and it's the same as Amazon does. Each virtual machine has a unique
#    domain name which can be used for communications. It's not too easy to maintain, but enterprise companies prefer
#    this way (I don't know why)
#
# Thus, this function resolves the way to communicate with instances based on input options.
def get_address_by_instance_object(node, opts):
    comm_method = opts.openstack_network_communication_method
    if comm_method == "fixed":
        for address in node.addresses["private"]:
            if address["OS-EXT-IPS:type"] == "fixed":
                return address["addr"]
        # if we get here, then instance doesn't have an address => error
        print >> stderr, "Instance didn't get fixed address, that's an error."
        sys.exit(1)
    elif comm_method == "floating":
        print node.addresses
        for address in node.addresses["private"]:
            if address["OS-EXT-IPS:type"] == "floating":
                return address["addr"]
        # if we get here, then instance doesn't have an address => error
        print >> stderr, "Instance didn't get floating address, that's an error."
        sys.exit(1)
    elif comm_method == "public-dns-address":
        if node.accessIPv4 == '':
            print >> stderr, "Instance didn't get public dns name, that's an error."
            sys.exit(1)
        else:
            return node.accessIPv4
    else:
        print >> stderr, "Something is totally wrong at get_address_by_instance_object, assert" + node.__dict__
        sys.exit(1)


# Deploy configuration files and run setup scripts on a newly launched
# or started EC2 cluster.
def setup_cluster(conn, master_nodes, slave_nodes, zoo_nodes, opts, deploy_ssh_key):
    master_nodes = update_instances(conn, master_nodes)
    slave_nodes = update_instances(conn, slave_nodes)
    zoo_nodes = update_instances(conn, zoo_nodes)
    master = get_address_by_instance_object(node=master_nodes[0], opts=opts)
    if deploy_ssh_key:
        print "Copying SSH key %s to master..." % opts.identity_file
        ssh(master, opts, 'mkdir -p ~/.ssh')
        scp(master, opts, opts.identity_file, '~/.ssh/id_rsa')
        ssh(master, opts, 'chmod 600 ~/.ssh/id_rsa')

    if opts.cluster_type == "mesos":
        modules = ['ephemeral-hdfs', 'persistent-hdfs', 'mesos']
    elif opts.cluster_type == "standalone":
        modules = ['ephemeral-hdfs', 'persistent-hdfs', 'spark-standalone']

    if opts.ganglia:
        modules.append('ganglia')

    ssh(master, opts, "git clone https://github.com/ispras/spark-openstack.git")

    print "Deploying files to master..."
    deploy_files(conn, "deploy.generic", opts, master_nodes, slave_nodes,
                 zoo_nodes, modules)

    print "Running setup on master..."

    setup_spark_cluster(master, opts)
    print "Done!"


def setup_mesos_cluster(master, opts):
    ssh(master, opts, "chmod u+x mesos-openstack/setup")
    ssh(master, opts, "mesos-openstack/setup %s %s %s %s" %
                      ("generic", "none", "master", opts.swap))


def setup_standalone_cluster(master, slave_nodes, opts):
    slave_ips = '\n'.join([get_address_by_instance_object(i,opts) for i in slave_nodes])
    ssh(master, opts, "echo \"%s\" > spark/conf/slaves" % (slave_ips))
    ssh(master, opts, "/root/spark/bin/start-all.sh")


def setup_spark_cluster(master, opts):
    ssh(master, opts, "chmod u+x spark-openstack/setup.sh")
    ssh(master, opts, "spark-openstack/setup.sh")
    if opts.cluster_type == "mesos":
        print "Mesos cluster started at http://%s:8080" % master
    elif opts.cluster_type == "standalone":
        print "Spark standalone cluster started at http://%s:8080" % master

    if opts.ganglia:
        print "Ganglia started at http://%s:5080/ganglia" % master


# Wait for a whole cluster (masters, slaves and ZooKeeper) to start up
def wait_for_cluster(conn, wait_secs, master_nodes, slave_nodes, zoo_nodes):
    print "Waiting for instances to start up..."
    time.sleep(5)
    wait_for_instances(conn, master_nodes)
    wait_for_instances(conn, slave_nodes)
    if zoo_nodes != []:
        wait_for_instances(conn, zoo_nodes)
    print "Waiting %d more seconds..." % wait_secs
    time.sleep(wait_secs)


# Deploy the configuration file templates in a given local directory to
# a cluster, filling in any template parameters with information about the
# cluster (e.g. lists of masters and slaves). Files are only deployed to
# the first master instance in the cluster, and we expect the setup
# script to be run on that instance to copy them to other nodes.
def deploy_files(conn, root_dir, opts, master_nodes, slave_nodes, zoo_nodes,
                 modules):
#TODO: check if configurations with "public" addresses exist (or maybe return public_dns_name)
    master_nodes = update_instances(conn, master_nodes)
    slave_nodes = update_instances(conn, slave_nodes)
    zoo_nodes = update_instances(conn, zoo_nodes)
    active_master = get_address_by_instance_object(node=master_nodes[0], opts=opts)

    num_disks = get_num_disks(opts.instance_type)
    hdfs_data_dirs = "/mnt/ephemeral-hdfs/data"
    mapred_local_dirs = "/mnt/hadoop/mrlocal"
    spark_local_dirs = "/mnt/spark"
    if num_disks > 1:
        for i in range(2, num_disks + 1):
            hdfs_data_dirs += ",/mnt%d/ephemeral-hdfs/data" % i
            mapred_local_dirs += ",/mnt%d/hadoop/mrlocal" % i
            spark_local_dirs += ",/mnt%d/spark" % i

# TODO: find out if we need zoo_nodes at all. Now here's a hack
    zoo_nodes = []
    if zoo_nodes != []:
        zoo_list = "NONE"
        print >> stderr, "Something is totally wrong, assert. Zoo-nodes should be empty (now)"
        sys.exit(1)
#        zoo_list = '\n'.join([i.public_dns_name for i in zoo_nodes])
#        cluster_url = "zoo://" + ",".join(
#            ["%s:2181/mesos" % i.public_dns_name for i in zoo_nodes])
    elif opts.cluster_type == "mesos":
        zoo_list = "NONE"
        cluster_url = "%s:5050" % active_master
    elif opts.cluster_type == "standalone":
        zoo_list = "NONE"
        cluster_url = "%s:7077" % active_master

    template_vars = {
        "master_list": '\n'.join([get_address_by_instance_object(i, opts) for i in master_nodes]),
        "active_master": active_master,
        "slave_list": '\n'.join([get_address_by_instance_object(i, opts) for i in slave_nodes]),
        "zoo_list": zoo_list,
        "cluster_url": cluster_url,
        "hdfs_data_dirs": hdfs_data_dirs,
        "mapred_local_dirs": mapred_local_dirs,
        "spark_local_dirs": spark_local_dirs,
        "swap": str(opts.swap),
        "modules": '\n'.join(modules)
    }

    # Create a temp directory in which we will place all the files to be
    # deployed after we substitue template parameters in them
    tmp_dir = tempfile.mkdtemp()
    for path, dirs, files in os.walk(root_dir):
        if path.find(".svn") == -1:
            dest_dir = os.path.join('/', path[len(root_dir):])
            local_dir = tmp_dir + dest_dir
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            for filename in files:
                if filename[0] not in '#.~' and filename[-1] != '~':
                    dest_file = os.path.join(dest_dir, filename)
                    local_file = tmp_dir + dest_file
                    with open(os.path.join(path, filename)) as src:
                        with open(local_file, "w") as dest:
                            text = src.read()
                            for key in template_vars:
                                text = text.replace("{{" + key + "}}", template_vars[key])
                            dest.write(text)
                            dest.close()
        # rsync the whole directory over to the master machine

    print ("!!!!Going to rsync configuration")
    print (root_dir)
    command = (("rsync -rv -e 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s' " +
                "'%s/' '%s@%s:/'") % (opts.identity_file, tmp_dir, opts.user, active_master))
    print ("!!!!Rsynced configuration, details: id_file %s , tmp_dir: %s, user: %s, active_master: %s"  )  % (opts.identity_file, tmp_dir, opts.user, active_master)
    print (command)
    print template_vars
    subprocess.check_call(command, shell=True)
    # Remove the temp directory we created above
#    shutil.rmtree(tmp_dir)


# Copy a file to a given host through scp, throwing an exception if scp fails
def scp(host, opts, local_file, dest_file):
    subprocess.check_call(
        "scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s '%s' '%s@%s:%s'" %
        (opts.identity_file, local_file, opts.user, host, dest_file), shell=True)


# Run a command on a host through ssh, retrying up to two times
# and then throwing an exception if ssh continues to fail.
def ssh(host, opts, command):
    tries = 0
    while True:
        try:
            return subprocess.check_call(
                "ssh -t -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s %s@%s '%s'" %
                (opts.identity_file, opts.user, host, command), shell=True)
        except subprocess.CalledProcessError as e:
            if (tries > 2):
                raise e
            print "Error connecting to host {0}, sleeping 30".format(e)
            time.sleep(30)
            tries += 1


def main():
    (opts, action, cluster_name) = parse_args()
    try:
        conn = client.Client(opts.openstack_login,
                                    opts.openstack_password,
                                    opts.openstack_tenant_name,
                                    opts.openstack_address,
                                    service_type="compute")
        conn.authenticate()

    except Exception as e:
        print >> stderr, (e)
        sys.exit(1)

    if action == "launch":
        if opts.resume:
            (master_nodes, slave_nodes, zoo_nodes) = get_existing_cluster(
                conn, opts, cluster_name)
        else:
            (master_nodes, slave_nodes, zoo_nodes) = launch_cluster(
                conn, opts, cluster_name)
            wait_for_cluster(conn, opts.wait, master_nodes, slave_nodes, zoo_nodes)
            master_nodes = update_instances(conn,master_nodes)
            slave_nodes = update_instances(conn,slave_nodes)
            zoo_nodes = update_instances(conn,zoo_nodes)
        setup_cluster(conn, master_nodes, slave_nodes, zoo_nodes, opts, True)

    elif action == "destroy":
        response = raw_input("Are you sure you want to destroy the cluster " +
                             cluster_name + "?\nALL DATA ON ALL NODES WILL BE LOST!!\n" +
                             "Destroy cluster " + cluster_name + " (y/N): ")
        if response == "y":
            (master_nodes, slave_nodes, zoo_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            print "Terminating master..."
            for inst in master_nodes:
                inst.delete()
            print "Terminating slaves..."
            for inst in slave_nodes:
                inst.delete()
            if zoo_nodes != []:
                print "Terminating zoo..."
                for inst in zoo_nodes:
                    inst.delete()

            # Delete security groups as well
            if opts.delete_groups:
                print "Deleting security groups (this will take some time)..."
                group_names = [cluster_name + "-master", cluster_name + "-slaves", cluster_name + "-zoo"]

                attempt = 1;
                while attempt <= 3:
                    print "Attempt %d" % attempt
                    groups = [g for g in conn.security_groups.list() if g.name in group_names]
                    success = True
                    # Delete individual rules in all groups before deleting groups to
                    # remove dependencies between them
                    for group in groups:
                        print "Deleting rules in security group " + group.name
                        for rule in group.rules:
                            try:
                                conn.security_group_rules.delete(rule=rule["id"])
                            except Exception as e:
                                print >> stderr, (e)
                                sys.exit(1)

                    # Sleep for AWS eventual-consistency to catch up, and for instances
                    # to terminate
                    time.sleep(30)  # Yes, it does have to be this long :-(
                    for group in groups:
                        try:
                            conn.security_groups.delete(group=group.name)
                            print "Deleted security group " + group.name
                        except Exception as e:
                            print >> stderr, (e)
                            print "Failed to delete security group " + group.name
                            sys.exit(1)

                    # Unfortunately, group.revoke() returns True even if a rule was not
                    # deleted, so this needs to be rerun if something fails
                    if success:
                        break

                    attempt += 1

                if not success:
                    print "Failed to delete all security groups after 3 tries."
                    print "Try re-running in a few minutes."

    elif action == "login":
        (master_nodes, slave_nodes, zoo_nodes) = get_existing_cluster(
            conn, opts, cluster_name)
        master = get_address_by_instance_object(node=master_nodes[0],opts=opts)
        print "Logging into master " + master + "..."
        proxy_opt = ""
        if opts.proxy_port != None:
            proxy_opt = "-D " + opts.proxy_port
        subprocess.check_call("ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i %s %s %s@%s" %
                              (opts.identity_file, proxy_opt, opts.user, master), shell=True)

    elif action == "get-master":
        (master_nodes, slave_nodes, zoo_nodes) = get_existing_cluster(conn, opts, cluster_name)
        print get_address_by_instance_object(node=master_nodes[0], opts=opts)

    elif action == "stop":
        response = raw_input("Are you sure you want to stop the cluster " +
                             cluster_name + "?\nDATA ON EPHEMERAL DISKS WILL BE LOST, " +
                             "BUT THE CLUSTER WILL KEEP USING SPACE ON\n" +
                             "AMAZON EBS IF IT IS EBS-BACKED!!\n" +
                             "Stop cluster " + cluster_name + " (y/N): ")
        if response == "y":
            (master_nodes, slave_nodes, zoo_nodes) = get_existing_cluster(
                conn, opts, cluster_name, die_on_error=False)
            print "Stopping master..."
            for inst in master_nodes:
                if inst.status not in ["STOP", "DELETED"]:
                    inst.stop()
            print "Stopping slaves..."
            for inst in slave_nodes:
                if inst.status not in ["STOP", "DELETED"]:
                    inst.stop()
            if zoo_nodes != []:
                print "Stopping zoo..."
                for inst in zoo_nodes:
                    if inst.status not in ["STOP", "DELETED"]:
                        inst.stop()

    elif action == "start":
        (master_nodes, slave_nodes, zoo_nodes) = get_existing_cluster(
            conn, opts, cluster_name)
        print "Starting slaves..."
        for inst in slave_nodes:
            if inst.state not in ["DELETED"]:
                inst.start()
        print "Starting master..."
        for inst in master_nodes:
            if inst.state not in ["DELETED"]:
                inst.start()
        if zoo_nodes != []:
            print "Starting zoo..."
            for inst in zoo_nodes:
                if inst.status not in ["DELETED"]:
                    inst.start()
        wait_for_cluster(conn, opts.wait, master_nodes, slave_nodes, zoo_nodes)
        master_nodes = update_instances(conn, master_nodes)
        slave_nodes = update_instances(conn, slave_nodes)
        zoo_nodes = update_instance(conn, zoo_nodes)
        setup_cluster(conn, master_nodes, slave_nodes, zoo_nodes, opts, False)

    else:
        print >> stderr, "Invalid action: %s" % action
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig()
    main()
