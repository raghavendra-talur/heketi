//
// Copyright (c) 2015 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

package glusterfs

import (
	"github.com/boltdb/bolt"
	"github.com/heketi/heketi/executors"
	"github.com/lpabon/godbc"
)

func (v *BlockVolumeEntry) createBlockVolume(db *bolt.DB,
	executor executors.Executor, blockHostingVolume string) error {

	godbc.Require(db != nil)
	godbc.Require(blockHostingVolume != "")

	vr, host, err := v.createBlockVolumeRequest(db, blockHostingVolume)
	if err != nil {
		return err
	}

	// Create the volume
	blockVolumeInfo, err := executor.BlockVolumeCreate(host, vr)
	if err != nil {
		return err
	}

	v.Info.BlockVolume.Iqn = blockVolumeInfo.Iqn
	v.Info.BlockVolume.Hosts = blockVolumeInfo.BlockHosts
	v.Info.BlockVolume.Lun = 0

	return nil
}

func (v *BlockVolumeEntry) createBlockVolumeRequest(db *bolt.DB,
	blockHostingVolume string) (*executors.BlockVolumeRequest, string, error) {
	godbc.Require(db != nil)
	godbc.Require(blockHostingVolume != "")

	// Setup list of bricks
	vr := &executors.BlockVolumeRequest{}
	var executorhost string

	// TODO -- which NodeId
	err := db.View(func(tx *bolt.Tx) error {
		bhvol, err := NewVolumeEntryFromId(tx, blockHostingVolume)
		if err != nil {
			return err
		}

		v.Info.Cluster = bhvol.Info.Cluster

		var nodeId string

		for _, nodeId = range bhvol.Info.Mount.GlusterFS.Hosts {
			// Check if glusterd is up here.
		}

		node, err := NewNodeEntryFromId(tx, nodeId)
		if err != nil {
			return err
		}

		if executorhost == "" {
			executorhost = node.ManageHostName()
		}

		return nil
	})
	if err != nil {
		logger.Err(err)
		return nil, "", err
	}

	// Setup volume information in the request
	vr.Name = v.Info.Name
	vr.BlockHosts = v.Info.BlockVolume.Hosts
	vr.GlusterVolumeName = blockHostingVolume
	vr.Hacount = v.Info.Hacount
	vr.Size = v.Info.Size

	return vr, executorhost, nil
}
