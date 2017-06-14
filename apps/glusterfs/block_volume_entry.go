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
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/heketi/heketi/executors"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/heketi/heketi/pkg/utils"
	"github.com/lpabon/godbc"
)

type BlockVolumeEntry struct {
	Info api.BlockVolumeInfo
}

func BlockVolumeList(tx *bolt.Tx) ([]string, error) {
	list := EntryKeys(tx, BOLTDB_BUCKET_BLOCKVOLUME)
	if list == nil {
		return nil, ErrAccessList
	}
	return list, nil
}

func NewBlockHostingVolume(db *bolt.DB, executor executors.Executor, allocator Allocator, clusters []string) (*VolumeEntry, error) {
	var msg api.VolumeCreateRequest
	var err error

	msg.Clusters = clusters
	msg.Durability.Type = api.DurabilityReplicate
	msg.Size = NewBlockHostingVolumeSize
	msg.Durability.Replicate.Replica = 3

	vol := NewVolumeEntryFromRequest(&msg)

	if uint64(msg.Size)*GB < vol.Durability.MinVolumeSize() {
		return nil, fmt.Errorf("Requested volume size (%v GB) is "+
			"smaller than the minimum supported volume size (%v)",
			msg.Size, vol.Durability.MinVolumeSize())
	}

	err = vol.Create(db, executor, allocator)
	if err != nil {
		logger.LogError("Failed to create Block Hosting Volume: %v", err)
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		vol.Info.BlockInfo.FreeSize = vol.Info.Size
		vol.Info.Block = true

		err = vol.Save(tx)
		if err != nil {
			return err
		}

		return err
	})

	if err != nil {
		logger.LogError("Failed to save the Block Hosting volume settings: %v", err)
		err = vol.Destroy(db, executor)
		if err != nil {
			logger.LogError("Failed to destroy the failed Block Hosting volume: %v", err)
		}

	}

	logger.Info("Block Hosting Volume created %v", vol.Info.Name)

	return vol, err

}

func NewBlockVolumeEntry() *BlockVolumeEntry {
	entry := &BlockVolumeEntry{}

	return entry
}

func NewBlockVolumeEntryFromRequest(req *api.BlockVolumeCreateRequest) *BlockVolumeEntry {
	godbc.Require(req != nil)

	vol := NewBlockVolumeEntry()
	vol.Info.Id = utils.GenUUID()
	vol.Info.Size = req.Size

	if req.Name == "" {
		vol.Info.Name = "vol_" + vol.Info.Id
	} else {
		vol.Info.Name = req.Name
	}

	// If Clusters is zero, then it will be assigned during volume creation
	vol.Info.Clusters = req.Clusters
	vol.Info.Hacount = req.Hacount

	return vol
}

func NewBlockVolumeEntryFromId(tx *bolt.Tx, id string) (*BlockVolumeEntry, error) {
	godbc.Require(tx != nil)

	entry := NewBlockVolumeEntry()
	err := EntryLoad(tx, entry, id)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (v *BlockVolumeEntry) BucketName() string {
	return BOLTDB_BUCKET_BLOCKVOLUME
}

func (v *BlockVolumeEntry) Save(tx *bolt.Tx) error {
	godbc.Require(tx != nil)
	godbc.Require(len(v.Info.Id) > 0)

	return EntrySave(tx, v, v.Info.Id)
}

func (v *BlockVolumeEntry) Delete(tx *bolt.Tx) error {
	return EntryDelete(tx, v, v.Info.Id)
}

func (v *BlockVolumeEntry) NewInfoResponse(tx *bolt.Tx) (*api.BlockVolumeInfoResponse, error) {
	godbc.Require(tx != nil)

	info := api.NewBlockVolumeInfoResponse()
	info.Id = v.Info.Id
	info.Cluster = v.Info.Cluster
	info.BlockVolume = v.Info.BlockVolume
	info.Size = v.Info.Size
	info.Name = v.Info.Name
	info.Hacount = v.Info.Hacount
	info.BlockHostingVolume = v.Info.BlockHostingVolume

	return info, nil
}

func (v *BlockVolumeEntry) Marshal() ([]byte, error) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	err := enc.Encode(*v)

	return buffer.Bytes(), err
}

func (v *BlockVolumeEntry) Unmarshal(buffer []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(buffer))
	err := dec.Decode(v)
	if err != nil {
		return err
	}

	return nil
}

func (v *BlockVolumeEntry) Create(db *bolt.DB,
	executor executors.Executor,
	allocator Allocator) (e error) {

	// On any error, remove the volume
	defer func() {
		if e != nil {
			db.Update(func(tx *bolt.Tx) error {
				v.Delete(tx)

				return nil
			})
		}
	}()

	var possibleClusters []string
	if len(v.Info.Clusters) == 0 {
		err := db.View(func(tx *bolt.Tx) error {
			var err error
			possibleClusters, err = ClusterList(tx)
			return err
		})
		if err != nil {
			return err
		}
	} else {
		possibleClusters = v.Info.Clusters
	}

	//
	// If there are any clusters marked with the Block
	// flag, then only consider those. Otherwise consider
	// all clusters.
	//
	var blockClusters []string
	for _, clusterId := range possibleClusters {
		err := db.View(func(tx *bolt.Tx) error {
			c, err := NewClusterEntryFromId(tx, clusterId)
			if err != nil {
				return err
			}
			if c.Info.Block {
				blockClusters = append(blockClusters, clusterId)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if blockClusters != nil {
		possibleClusters = blockClusters
	}

	if len(possibleClusters) == 0 {
		logger.LogError("BlockVolume being ask to be created, but there are no clusters configured")
		return ErrNoSpace
	}
	logger.Debug("Using the following clusters: %+v", possibleClusters)

	var possibleVolumes []string
	for _, clusterId := range possibleClusters {
		err := db.View(func(tx *bolt.Tx) error {
			var err error
			c, err := NewClusterEntryFromId(tx, clusterId)
			for _, vol := range c.Info.Volumes {
				volEntry, err := NewVolumeEntryFromId(tx, vol)
				if err != nil {
					return err
				}
				if volEntry.Info.Block {
					possibleVolumes = append(possibleVolumes, vol)
				}
			}
			return err
		})
		if err != nil {
			return err
		}
	}

	var volumes []string
	for _, vol := range possibleVolumes {
		err := db.View(func(tx *bolt.Tx) error {
			volEntry, err := NewVolumeEntryFromId(tx, vol)
			if volEntry.Info.BlockInfo.FreeSize >= v.Info.Size {
				for _, blockvol := range volEntry.Info.BlockInfo.BlockVolumes {
					bv, err := NewBlockVolumeEntryFromId(tx, blockvol)
					if err != nil {
						return err
					}
					if v.Info.Name == bv.Info.Name {
						return fmt.Errorf("Name %v already in use in file volume %v",
							v.Info.Name, volEntry.Info.Name)
					}
				}
			}
			return err
		})
		if err != nil {
			logger.Warning("%v", err.Error())
		} else {
			volumes = append(volumes, vol)
		}
	}

	var blockHostingVolume string
	if len(volumes) == 0 {
		logger.Info("No block hosting volumes found in the cluster list")
		bhvol, err := NewBlockHostingVolume(db, executor, allocator, v.Info.Clusters)
		if err != nil {
			return err
		}
		blockHostingVolume = bhvol.Info.Id
	} else {
		blockHostingVolume = volumes[0]
	}

	defer func() {
		if e != nil {
			db.Update(func(tx *bolt.Tx) error {
				// removal of stuff that was created in the db
				return nil
			})
		}
	}()

	// Cluster -> Volume (gluster-volume) -> BlockVolume
	// Create gluster-block volume - this calls gluster_block
	// TODO...
	err := v.createBlockVolume(db, executor, blockHostingVolume)
	if err != nil {
		return err
	}

	defer func() {
		if e != nil {
			v.Destroy(db, executor)
		}
	}()

	err = db.Update(func(tx *bolt.Tx) error {

		err = v.Save(tx)
		if err != nil {
			return err
		}

		cluster, err := NewClusterEntryFromId(tx, v.Info.Cluster)
		if err != nil {
			return err
		}

		cluster.VolumeAdd(v.Info.Id)

		err = cluster.Save(tx)
		if err != nil {
			return err
		}
		volume, err := NewVolumeEntryFromId(tx, blockHostingVolume)
		if err != nil {
			return err
		}

		volume.BlockVolumeAdd(v.Info.Id)
		err = volume.Save(tx)
		if err != nil {
			return err
		}

		return err
		// TODO:
		//  do we need to save the cluster? do we store anything in
		//  the cluster
		//  [ashiq]Yes, We save the Volume ids list which belongs to the cluster
	})
	if err != nil {
		return err
	}

	return nil
}

func (v *BlockVolumeEntry) Destroy(db *bolt.DB, executor executors.Executor) error {
	logger.Info("Destroying volume %v", v.Info.Id)

	var executorhost string
	var NodeId string
	var blockHostingVolumeName string
	for _, NodeId = range v.Info.BlockVolume.Hosts {
		// Check glusterd/gluster-blockd here
	}
	db.View(func(tx *bolt.Tx) error {
		if executorhost == "" {
			node, err := NewNodeEntryFromId(tx, NodeId)
			if err != nil {
				logger.LogError("Unable to determine brick node: %v", err)
				return err
			}
			executorhost = node.ManageHostName()
		}
		volume, err := NewVolumeEntryFromId(tx, v.Info.BlockHostingVolume)
		if err != nil {
			logger.LogError("Unable to determine brick node: %v", err)
			return err
		}
		blockHostingVolumeName = volume.Info.Name
		return nil
	})

	// Determine if we can destroy the volume
	// [ashiq] we can skip this part for now as there is nothing to verify as we dont have snapshotting yet
	/*
		err := executor.BlockVolumeDestroyCheck(executorhost, v.Info.Name)
		if err != nil {
			logger.Err(err)
			return err
		}
	*/
	// :TODO: What if the host is no longer available, we may need to try others
	// (here we call gluster_block destroy)
	err := executor.BlockVolumeDestroy(executorhost, blockHostingVolumeName, v.Info.Name)
	if err != nil {
		logger.LogError("Unable to delete volume: %v", err)
		return err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		// Remove volume from cluster
		cluster, err := NewClusterEntryFromId(tx, v.Info.Cluster)
		if err != nil {
			logger.Err(err)
			// Do not return here.. keep going
		}
		cluster.VolumeDelete(v.Info.Id)
		err = cluster.Save(tx)
		if err != nil {
			logger.Err(err)
			// Do not return here.. keep going
		}

		blockHostingVolume, err := NewVolumeEntryFromId(tx, v.Info.BlockHostingVolume)
		if err != nil {
			logger.Err(err)
			// Do not return here.. keep going
		}

		blockHostingVolume.BlockVolumeDelete(v.Info.Id)
		if err != nil {
			logger.Err(err)
			// Do not return here.. keep going
		}
		blockHostingVolume.Save(tx)

		if err != nil {
			logger.Err(err)
			// Do not return here.. keep going
		}

		v.Delete(tx)

		return nil
	})

	return err
}
