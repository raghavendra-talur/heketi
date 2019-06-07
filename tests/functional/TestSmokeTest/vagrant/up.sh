#!/bin/sh

set -e
vagrant up --no-provision --no-parallel "$@"
vagrant provision

virsh list --name --all | grep "heketi-vm" | while read each;
do
        echo  domain "$each";
	virsh snapshot-create-as --domain "$each" --atomic --name "deployed"
done

