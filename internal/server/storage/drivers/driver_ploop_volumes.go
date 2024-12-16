package drivers

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/lxc/incus/v6/internal/instancewriter"
	"github.com/lxc/incus/v6/internal/server/backup"
	"github.com/lxc/incus/v6/internal/server/migration"
	"github.com/lxc/incus/v6/internal/server/operations"
	"github.com/lxc/incus/v6/shared/revert"
	"github.com/lxc/incus/v6/shared/units"
	"github.com/lxc/incus/v6/shared/util"
)

// CreateVolume creates an empty volume and can optionally fill it by executing the supplied
// filler function.
func (d *ploop) CreateVolume(vol Volume, filler *VolumeFiller, op *operations.Operation) error {
	volPath := vol.MountPath()

	revert := revert.New()
	defer revert.Fail()

	if util.PathExists(vol.MountPath()) {
		return fmt.Errorf("Volume path %q already exists", vol.MountPath())
	}

	// Create the volume itself.
	err := vol.EnsureMountPath()
	if err != nil {
		return err
	}

	revert.Add(func() { _ = os.RemoveAll(volPath) })

	// Get path to disk volume if volume is block or iso.
	rootBlockPath := ""
	if IsContentBlock(vol.contentType) {
		// We expect the filler to copy the VM image into this path.
		rootBlockPath, err = d.GetVolumeDiskPath(vol)
		if err != nil {
			return err
		}
	}
	// else if vol.volType != VolumeTypeBucket {
	// 	// Filesystem quotas only used with non-block volume types.
	// 	revertFunc, err := d.setupInitialQuota(vol)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	if revertFunc != nil {
	// 		revert.Add(revertFunc)
	// 	}
	// }

	// Run the volume filler function if supplied.
	err = d.runFiller(vol, rootBlockPath, filler, false)
	if err != nil {
		return err
	}

	// If we are creating a block volume, resize it to the requested size or the default.
	// For block volumes, we expect the filler function to have converted the qcow2 image to raw into the rootBlockPath.
	// For ISOs the content will just be copied.
	if IsContentBlock(vol.contentType) {
		// Convert to bytes.
		sizeBytes, err := units.ParseByteSizeString(vol.ConfigSize())
		if err != nil {
			return err
		}

		// Ignore ErrCannotBeShrunk when setting size this just means the filler run above has needed to
		// increase the volume size beyond the default block volume size.
		_, err = ensureVolumeBlockFile(vol, rootBlockPath, sizeBytes, false)
		if err != nil && !errors.Is(err, ErrCannotBeShrunk) {
			return err
		}

		// Move the GPT alt header to end of disk if needed and if filler specified.
		if vol.IsVMBlock() && filler != nil && filler.Fill != nil {
			err = d.moveGPTAltHeader(rootBlockPath)
			if err != nil {
				return err
			}
		}
	}

	revert.Success()
	return nil
}

// DeleteVolume deletes a volume of the storage device. If any snapshots of the volume remain then
// this function will return an error.
func (d *ploop) DeleteVolume(vol Volume, op *operations.Operation) error {
	snapshots, err := d.VolumeSnapshots(vol, op)
	if err != nil {
		return err
	}

	if len(snapshots) > 0 {
		return fmt.Errorf("Cannot remove a volume that has snapshots")
	}

	volPath := vol.MountPath()

	// If the volume doesn't exist, then nothing more to do.
	if !util.PathExists(volPath) {
		return nil
	}

	// Get the volume ID for the volume, which is used to remove project quota.
	// if vol.Type() != VolumeTypeBucket {
	// 	volID, err := d.getVolID(vol.volType, vol.name)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	// Remove the project quota.
	// 	// err = d.deleteQuota(volPath, volID)
	// 	// if err != nil {
	// 	// 	return err
	// 	// }
	// }

	// Remove the volume from the storage device.
	err = forceRemoveAll(volPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("Failed to remove '%s': %w", volPath, err)
	}

	// Although the volume snapshot directory should already be removed, lets remove it here
	// to just in case the top-level directory is left.
	err = deleteParentSnapshotDirIfEmpty(d.name, vol.volType, vol.name)
	if err != nil {
		return err
	}

	return nil
}

// HasVolume indicates whether a specific volume exists on the storage pool.
func (d *ploop) HasVolume(vol Volume) (bool, error) {
	return genericVFSHasVolume(vol)
}

// FillVolumeConfig populate volume with default config.
func (d *ploop) FillVolumeConfig(vol Volume) error {
	initialSize := vol.config["size"]

	err := d.fillVolumeConfig(&vol)
	if err != nil {
		return err
	}

	// Buckets do not support default volume size.
	// If size is specified manually, do not remove, so it triggers validation failure and an error to user.
	if vol.volType == VolumeTypeBucket && initialSize == "" {
		delete(vol.config, "size")
	}

	return nil
}

// ValidateVolume validates the supplied volume config. Optionally removes invalid keys from the volume's config.
func (d *ploop) ValidateVolume(vol Volume, removeUnknownKeys bool) error {
	err := d.validateVolume(vol, nil, removeUnknownKeys)
	if err != nil {
		return err
	}

	if vol.config["size"] != "" && vol.volType == VolumeTypeBucket {
		return fmt.Errorf("Size cannot be specified for buckets")
	}

	return nil
}

// CreateVolumeFromBackup restores a backup tarball onto the storage device.
func (d *ploop) CreateVolumeFromBackup(vol Volume, srcBackup backup.Info, srcData io.ReadSeeker, op *operations.Operation) (VolumePostHook, revert.Hook, error) {
	return nil, nil, nil
}

// CreateVolumeFromCopy provides same-pool volume copying functionality.
func (d *ploop) CreateVolumeFromCopy(vol Volume, srcVol Volume, copySnapshots bool, allowInconsistent bool, op *operations.Operation) error {
	return nil
}

// CreateVolumeFromMigration creates a volume being sent via a migration.
func (d *ploop) CreateVolumeFromMigration(vol Volume, conn io.ReadWriteCloser, volTargetArgs migration.VolumeTargetArgs, preFiller *VolumeFiller, op *operations.Operation) error {
	return nil
}

// RefreshVolume provides same-pool volume and specific snapshots syncing functionality.
func (d *ploop) RefreshVolume(vol Volume, srcVol Volume, srcSnapshots []Volume, allowInconsistent bool, op *operations.Operation) error {
	return nil
}

// UpdateVolume applies config changes to the volume.
func (d *ploop) UpdateVolume(vol Volume, changedConfig map[string]string) error {
	if vol.contentType != ContentTypeFS {
		return ErrNotSupported
	}

	_, changed := changedConfig["size"]
	if changed {
		err := d.SetVolumeQuota(vol, changedConfig["size"], false, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetVolumeUsage returns the disk space used by the volume.
func (d *ploop) GetVolumeUsage(vol Volume) (int64, error) {
	return 0, nil
}

// SetVolumeQuota applies a size limit on volume.
func (d *ploop) SetVolumeQuota(vol Volume, size string, allowUnsafeResize bool, op *operations.Operation) error {
	return nil
}

// GetVolumeDiskPath returns the location of a disk volume.
func (d *ploop) GetVolumeDiskPath(vol Volume) (string, error) {
	return "", nil
}

// ListVolumes returns a list of volumes in storage pool.
func (d *ploop) ListVolumes() ([]Volume, error) {
	return nil, nil
}

// MountVolume simulates mounting a volume.
func (d *ploop) MountVolume(vol Volume, op *operations.Operation) error {
	return nil
}

// UnmountVolume simulates unmounting a volume. As dir driver doesn't have volumes to unmount it
// returns false indicating the volume was already unmounted.
func (d *ploop) UnmountVolume(vol Volume, keepBlockDev bool, op *operations.Operation) (bool, error) {
	return false, nil
}

// RenameVolume renames a volume and its snapshots.
func (d *ploop) RenameVolume(vol Volume, newVolName string, op *operations.Operation) error {
	return nil
}

// MigrateVolume sends a volume for migration.
func (d *ploop) MigrateVolume(vol Volume, conn io.ReadWriteCloser, volSrcArgs *migration.VolumeSourceArgs, op *operations.Operation) error {
	return nil
}

// BackupVolume copies a volume (and optionally its snapshots) to a specified target path.
// This driver does not support optimized backups.
func (d *ploop) BackupVolume(vol Volume, tarWriter *instancewriter.InstanceTarWriter, optimized bool, snapshots []string, op *operations.Operation) error {
	return nil
}

// CreateVolumeSnapshot creates a snapshot of a volume.
func (d *ploop) CreateVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	return nil
}

// DeleteVolumeSnapshot removes a snapshot from the storage device. The volName and snapshotName
// must be bare names and should not be in the format "volume/snapshot".
func (d *ploop) DeleteVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	return nil
}

// MountVolumeSnapshot sets up a read-only mount on top of the snapshot to avoid accidental modifications.
func (d *ploop) MountVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	return nil
}

// UnmountVolumeSnapshot removes the read-only mount placed on top of a snapshot.
func (d *ploop) UnmountVolumeSnapshot(snapVol Volume, op *operations.Operation) (bool, error) {
	return true, nil
}

// VolumeSnapshots returns a list of snapshots for the volume (in no particular order).
func (d *ploop) VolumeSnapshots(vol Volume, op *operations.Operation) ([]string, error) {
	return nil, nil
}

// RestoreVolume restores a volume from a snapshot.
func (d *ploop) RestoreVolume(vol Volume, snapshotName string, op *operations.Operation) error {
	return nil
}

// RenameVolumeSnapshot renames a volume snapshot.
func (d *ploop) RenameVolumeSnapshot(snapVol Volume, newSnapshotName string, op *operations.Operation) error {
	return nil
}
