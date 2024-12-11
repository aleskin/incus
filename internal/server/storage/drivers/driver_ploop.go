package drivers

import (
	"io"

	"github.com/lxc/incus/v6/internal/instancewriter"
	"github.com/lxc/incus/v6/internal/server/backup"
	deviceConfig "github.com/lxc/incus/v6/internal/server/device/config"
	"github.com/lxc/incus/v6/internal/server/migration"
	"github.com/lxc/incus/v6/internal/server/operations"
	"github.com/lxc/incus/v6/shared/api"
	"github.com/lxc/incus/v6/shared/revert"
)

type ploop struct {
	common
}

// load is used to run one-time action per-driver rather than per-pool.
func (d *ploop) load() error {
	return nil
}

// Info returns info about the driver and its environment.
func (d *ploop) Info() Info {
	return Info{
		Name:                         "ploop",
		Version:                      "1",
		DefaultVMBlockFilesystemSize: deviceConfig.DefaultVMBlockFilesystemSize,
		OptimizedImages:              false,
		PreservesInodes:              false,
		Remote:                       d.isRemote(),
		VolumeTypes:                  []VolumeType{VolumeTypeCustom, VolumeTypeImage, VolumeTypeContainer, VolumeTypeVM},
		BlockBacking:                 false,
		RunningCopyFreeze:            true,
		DirectIO:                     true,
		MountedRoot:                  true,
	}
}

func (d *ploop) FillConfig() error {
	return nil
}

func (d *ploop) Create() error {
	return nil
}

func (d *ploop) Delete(op *operations.Operation) error {
	return nil
}

// Validate checks that all provide keys are supported and that no conflicting or missing configuration is present.
func (d *ploop) Validate(config map[string]string) error {
	return d.validatePool(config, nil, nil)
}

// Update applies any driver changes required from a configuration change.
func (d *ploop) Update(changedConfig map[string]string) error {
	return nil
}

// Mount mounts the storage pool.
func (d *ploop) Mount() (bool, error) {
	return true, nil
}

// Unmount unmounts the storage pool.
func (d *ploop) Unmount() (bool, error) {
	return true, nil
}

// GetResources returns the pool resource usage information.
func (d *ploop) GetResources() (*api.ResourcesStoragePool, error) {
	return nil, nil
}

// CreateVolume creates an empty volume and can optionally fill it by executing the supplied filler function.
func (d *ploop) CreateVolume(vol Volume, filler *VolumeFiller, op *operations.Operation) error {
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

// DeleteVolume deletes a volume of the storage device. If any snapshots of the volume remain then this function
// will return an error.
func (d *ploop) DeleteVolume(vol Volume, op *operations.Operation) error {
	return nil
}

// HasVolume indicates whether a specific volume exists on the storage pool.
func (d *ploop) HasVolume(vol Volume) (bool, error) {
	return true, nil
}

// ValidateVolume validates the supplied volume config. Optionally removes invalid keys from the volume's config.
func (d *ploop) ValidateVolume(vol Volume, removeUnknownKeys bool) error {
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
