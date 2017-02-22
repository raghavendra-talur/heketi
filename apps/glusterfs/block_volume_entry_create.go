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
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/heketi/heketi/executors"
	"github.com/heketi/heketi/pkg/utils"
	"github.com/lpabon/godbc"
	"strings"
)

func (v *VolumeEntry) createBlockVolume(db *bolt.DB,
	executor executors.Executor,
	blockVolumeName, size, host, gluster_volume_name) error {

	godbc.Require(db != nil)
	godbc.Require(brick_entries != nil)

	vr, host, err := v.createBlockVolumeRequest(db, ..)
	if err != nil {
		return err
	}

	// Create the volume
	_, err = executor.BlockVolumeCreate(host, vr) // TODO - implement
	if err != nil {
		return err
	}

	hosts := stringset.Strings()
	v.Info.BlockVolume.Hosts = hosts

	return nil
}

func (v *VolumeEntry) createVolumeRequest(db *bolt.DB,
	brick_entries []*BrickEntry) (*executors.VolumeRequest, string, error) {
	godbc.Require(db != nil)
	godbc.Require(brick_entries != nil)

	// Setup list of bricks
	vr := &executors.BlockVolumeRequest{}
	var executorhost string

	// TODO -- which NodeId
	err := db.View(func(tx *bolt.Tx) error {
		node, err := NewNodeEntryFromId(tx, NodeId)
		if err != nil {
			return err
		}

		if executorhost == "" {
			executorhost = node.ManageHostName()
		}
		vr.Bricks[i].Host = node.StorageHostName()
		godbc.Check(vr.Bricks[i].Host != "")

		return nil
	})
	if err != nil {
		logger.Err(err)
		return nil, "", err
	}

	// Setup volume information in the request
	vr.Name = v.Info.Name

	return vr, executorhost, nil
}
