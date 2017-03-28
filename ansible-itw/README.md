# ansible-odh

Ansible repository for the Operational Data Hub servers in PECHE and Edgeconnex

# Ansible requirements
Ansible version 2.2.0.0 should be used.

# Usage

Check the performed changes:

    ansible-playbook --check --diff -i lgi-staging site.yml

Apply the performed changes:

    ansible-playbook -i lgi-staging site.yml

# Getting started

Install squid on your workstation (it might be a VM from which you also run ansible):

    docker run -it -p 127.0.0.1:3128:3128 sameersbn/squid:3.3.8-20

Use the following ssh configuration in `.ssh/config`:

    Host 172.26.117.2? odhpoc??e
        RemoteForward 3128 127.0.0.1:3128

Add servers to `/etc/hosts`

    172.26.117.20 odhpoc01e
    172.26.117.21 odhpoc02e
    172.26.117.22 odhpoc03e
    172.26.117.23 odhpoc04e
    172.26.117.24 odhpoc05e
    172.26.117.25 odhpoc06e
    172.26.117.26 odhpoc07e
    172.26.117.27 odhpoc08e

    172.26.80.32 odhpoc01
    172.26.80.33 odhpoc02
    172.26.80.34 odhpoc03
    172.26.80.35 odhpoc04
    172.26.80.36 odhpoc05
    172.26.80.37 odhpoc06
    172.26.80.38 odhpoc07
    172.26.80.39 odhpoc08


To access internal network from your workstation, use `sshuttle`:

    sudo apt-get install sshuttle
    sshuttle -r odhuser@odhpoc01e 172.26.80.0/24

