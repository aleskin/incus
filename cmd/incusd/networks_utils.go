package main

import (
	"github.com/lxc/incus/internal/server/cluster"
	"github.com/lxc/incus/internal/server/db"
	"github.com/lxc/incus/internal/server/network"
	"github.com/lxc/incus/internal/server/state"
	"github.com/lxc/incus/shared/logger"
)

var networkOVNChassis *bool

func networkAutoAttach(cluster *db.Cluster, devName string) error {
	_, dbInfo, err := cluster.GetNetworkWithInterface(devName)
	if err != nil {
		// No match found, move on
		return nil
	}

	return network.AttachInterface(dbInfo.Name, devName)
}

// networkUpdateOVNChassis gets called on heartbeats to check if OVN needs reconfiguring.
func networkUpdateOVNChassis(s *state.State, heartbeatData *cluster.APIHeartbeat, localAddress string) error {
	// Check if we have at least one active OVN chassis.
	hasOVNChassis := false
	localOVNChassis := false
	for _, n := range heartbeatData.Members {
		for _, role := range n.Roles {
			if role == db.ClusterRoleOVNChassis {
				if n.Address == localAddress {
					localOVNChassis = true
				}

				hasOVNChassis = true
				break
			}
		}
	}

	runChassis := !hasOVNChassis || localOVNChassis
	if networkOVNChassis != nil && *networkOVNChassis != runChassis {
		// Detected that the local OVN chassis setup may be incorrect, restarting.
		err := networkRestartOVN(s)
		if err != nil {
			logger.Error("Error restarting OVN networks", logger.Ctx{"err": err})
		}
	}

	networkOVNChassis = &runChassis
	return nil
}