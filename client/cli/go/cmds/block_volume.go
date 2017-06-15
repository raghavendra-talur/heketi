//
// Copyright (c) 2015 The heketi Authors
//
// This file is licensed to you under your choice of the GNU Lesser
// General Public License, version 3 or any later version (LGPLv3 or
// later), or the GNU General Public License, version 2 (GPLv2), in all
// cases as published by the Free Software Foundation.
//

package cmds

import (
	"encoding/json"
	"errors"
	"fmt"
//	"os"
	"strings"

	client "github.com/heketi/heketi/client/api/go-client"
	"github.com/heketi/heketi/pkg/glusterfs/api"
	"github.com/spf13/cobra"
)

var (
	bv_size           int
	bv_volname        string
//	bv_gid            int64
	bv_clusters       string
	bv_id             string
)

func init() {
	RootCmd.AddCommand(blockVolumeCommand)
	blockVolumeCommand.AddCommand(blockVolumeCreateCommand)
	blockVolumeCommand.AddCommand(blockVolumeDeleteCommand)
	blockVolumeCommand.AddCommand(blockVolumeInfoCommand)
	blockVolumeCommand.AddCommand(blockVolumeListCommand)

	blockVolumeCreateCommand.Flags().IntVar(&size, "size", -1,
		"\n\tSize of volume in GB")
//	blockVolumeCreateCommand.Flags().Int64Var(&gid, "gid", 0,
//		"\n\tOptional: Initialize volume with the specified group id")
	blockVolumeCreateCommand.Flags().StringVar(&volname, "name", "",
		"\n\tOptional: Name of volume. Only set if really necessary")
	blockVolumeCreateCommand.Flags().StringVar(&clusters, "clusters", "",
		"\n\tOptional: Comma separated list of cluster ids where this volume"+
			"\n\tmust be allocated. If omitted, Heketi will allocate the volume"+
			"\n\ton any of the configured clusters which have the available space."+
			"\n\tProviding a set of clusters will ensure Heketi allocates storage"+
			"\n\tfor this volume only in the clusters specified.")
	blockVolumeCreateCommand.SilenceUsage = true
	blockVolumeDeleteCommand.SilenceUsage = true
	blockVolumeInfoCommand.SilenceUsage = true
	blockVolumeListCommand.SilenceUsage = true
}

var blockVolumeCommand = &cobra.Command{
	Use:   "blockvolume",
	Short: "Heketi Volume Management",
	Long:  "Heketi Volume Management",
}

var blockVolumeCreateCommand = &cobra.Command{
	Use:   "create",
	Short: "Create a GlusterFS block volume",
	Long:  "Create a GlusterFS block volume",
	Example: `  * Create a 100GB block volume
      $ heketi-cli blockvolume create --size=100

  * Create a 100GB block volume specifying two specific clusters:
      $ heketi-cli blockvolume create --size=100 \
        --clusters=0995098e1284ddccb46c7752d142c832,60d46d518074b13a04ce1022c8c7193c
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if size == -1 {
			return errors.New("Missing volume size")
		}

		var clusters_ []string
		if clusters != "" {
			clusters_ = strings.Split(clusters, ",")
		}

		req := &api.BlockVolumeCreateRequest{}
		req.Size = size
		req.Clusters = clusters_

//		if gid != 0 {
//			req.Gid = gid
//		}

		if volname != "" {
			req.Name = volname
		}

		heketi := client.NewClient(options.Url, options.User, options.Key)

		blockvolume, err := heketi.BlockVolumeCreate(req)
		if err != nil {
			return err
		}

		if options.Json {
			data, err := json.Marshal(blockvolume)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, string(data))
		} else {
			fmt.Fprintf(stdout, "%v", blockvolume)
		}

		return nil
	},
}

var blockVolumeDeleteCommand = &cobra.Command{
	Use:     "delete",
	Short:   "Deletes the volume",
	Long:    "Deletes the volume",
	Example: "  $ heketi-cli volume delete 886a86a868711bef83001",
	RunE: func(cmd *cobra.Command, args []string) error {
		s := cmd.Flags().Args()

		//ensure proper number of args
		if len(s) < 1 {
			return errors.New("Volume id missing")
		}

		//set volumeId
		volumeId := cmd.Flags().Arg(0)

		// Create a client
		heketi := client.NewClient(options.Url, options.User, options.Key)

		//set url
		err := heketi.BlockVolumeDelete(volumeId)
		if err == nil {
			fmt.Fprintf(stdout, "Volume %v deleted\n", volumeId)
		}

		return err
	},
}

var blockVolumeInfoCommand = &cobra.Command{
	Use:     "info",
	Short:   "Retreives information about the volume",
	Long:    "Retreives information about the volume",
	Example: "  $ heketi-cli volume info 886a86a868711bef83001",
	RunE: func(cmd *cobra.Command, args []string) error {
		//ensure proper number of args
		s := cmd.Flags().Args()
		if len(s) < 1 {
			return errors.New("Volume id missing")
		}

		// Set volume id
		volumeId := cmd.Flags().Arg(0)

		// Create a client to talk to Heketi
		heketi := client.NewClient(options.Url, options.User, options.Key)

		// Create cluster
		info, err := heketi.BlockVolumeInfo(volumeId)
		if err != nil {
			return err
		}

		if options.Json {
			data, err := json.Marshal(info)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, string(data))
		} else {
			fmt.Fprintf(stdout, "%v", info)
		}
		return nil

	},
}

var blockVolumeListCommand = &cobra.Command{
	Use:     "list",
	Short:   "Lists the volumes managed by Heketi",
	Long:    "Lists the volumes managed by Heketi",
	Example: "  $ heketi-cli blockvolume list",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Create a client
		heketi := client.NewClient(options.Url, options.User, options.Key)

		// List volumes
		list, err := heketi.BlockVolumeList()
		if err != nil {
			return err
		}

		if options.Json {
			data, err := json.Marshal(list)
			if err != nil {
				return err
			}
			fmt.Fprintf(stdout, string(data))
		} else {
			for _, id := range list.BlockVolumes {
				volume, err := heketi.BlockVolumeInfo(id)
				if err != nil {
					return err
				}

				fmt.Fprintf(stdout, "Id:%-35v Cluster:%-35v Name:%v\n",
					id,
					volume.Cluster,
					volume.Name)
			}
		}

		return nil
	},
}