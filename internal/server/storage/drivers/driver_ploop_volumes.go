package drivers

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"runtime"
	"strings"

	"bitbucket.org/aleskinprivate/vzgoploop"
	"github.com/lxc/incus/v6/internal/instancewriter"
	"github.com/lxc/incus/v6/internal/server/backup"
	"github.com/lxc/incus/v6/internal/server/migration"
	"github.com/lxc/incus/v6/internal/server/operations"
	"github.com/lxc/incus/v6/shared/logger"
	"github.com/lxc/incus/v6/shared/revert"
	"github.com/lxc/incus/v6/shared/units"
	"github.com/lxc/incus/v6/shared/util"
)

// TODO take values from vzgoploop
const defaultPloopSize = 4 * 512 * 1024
const defaultFileName = "root.hds"
const defaultDescriptor = "DiskDescriptor.xml"
const MaxTraceDepth = 5

func (d *ploop) PrintTrace(info string, depth int) {

	if depth > MaxTraceDepth {
		depth = MaxTraceDepth
	}

	if info != "" && depth > 1 {
		d.logger.Debug("VZ Ploop: Trace", logger.Ctx{"info": info, "depth": depth})
	}

	for i := 0; i < depth; i++ {
		pc, _, _, _ := runtime.Caller(depth - i)
		d.logger.Debug("VZ Ploop: Trace", logger.Ctx{"frame": depth - i, "func": strings.Replace(runtime.FuncForPC(pc).Name(),
			"github.com/lxc/incus/v6/internal/server/storage", "", 1)})
	}
}

// CreateVolume creates an empty volume and can optionally fill it by executing the supplied
// filler function.
func (d *ploop) CreateVolume(vol Volume, filler *VolumeFiller, op *operations.Operation) error {

	d.PrintTrace("", 1)
	volPath := vol.MountPath()
	d.logger.Debug("VZ Ploop: Create Volume", logger.Ctx{"MountPath": volPath, "Name": vol.name, "Type": vol.volType})

	revert := revert.New()
	defer revert.Fail()

	if util.PathExists(vol.MountPath()) {
		return fmt.Errorf("VZ Ploop: Volume path %q already exists", vol.MountPath())
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

	//create ploop device
	param := vzgoploop.VZP_CreateParam{
		Size:  defaultPloopSize,
		Image: volPath + "/" + defaultFileName,
	}

	res := vzgoploop.Create(&param)

	if res.Status != vzgoploop.VZP_SUCCESS {
		return fmt.Errorf("VZ Ploop: Can't create disk: %s \n", res.Msg)
	}

	disk, res := vzgoploop.Open(volPath + "/" + defaultDescriptor)
	if res.Status != vzgoploop.VZP_SUCCESS {
		return fmt.Errorf("VZ Ploop: Can't open disk: %s \n", res.Msg)
	}

	mp := vzgoploop.VZP_MountParam{Target: volPath + "/rootfs"}

	_ = os.Mkdir(mp.Target, 0755) //TODO
	device, res := disk.MountImage(&mp)
	if res.Status != vzgoploop.VZP_SUCCESS {
		return fmt.Errorf("VZ Ploop: Can't mount image create: %s \n", res.Msg)
	}

	d.logger.Info("VZ Ploop: Mounted", logger.Ctx{"device": device})

	// Run the volume filler function if supplied.
	err = d.runFiller(vol, rootBlockPath, filler, false)
	if err != nil {
		return err
	}

	res = disk.UmountImage()
	if res.Status != vzgoploop.VZP_SUCCESS {
		return fmt.Errorf("VZ Ploop: Can't umount image: %s \n", res.Msg)
	}

	disk.Close()

	//TODO and qemu VM?

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

	d.logger.Debug("VZ Ploop: Created Volume successfully\n")
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
	d.PrintTrace("", 1)
	return genericVFSHasVolume(vol)
}

// FillVolumeConfig populate volume with default config.
func (d *ploop) FillVolumeConfig(vol Volume) error {
	d.PrintTrace("", 1)

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
	d.PrintTrace("", 1)

	err := d.validateVolume(vol, nil, removeUnknownKeys)
	if err != nil {
		return err
	}

	if vol.config["size"] != "" && vol.volType == VolumeTypeBucket {
		return fmt.Errorf("VZ Ploop: Size cannot be specified for buckets")
	}

	return nil
}

// CreateVolumeFromBackup restores a backup tarball onto the storage device.
func (d *ploop) CreateVolumeFromBackup(vol Volume, srcBackup backup.Info, srcData io.ReadSeeker, op *operations.Operation) (VolumePostHook, revert.Hook, error) {
	d.PrintTrace("", 1)

	return nil, nil, nil
}

// CreateVolumeFromCopy provides same-pool volume copying functionality.
func (d *ploop) CreateVolumeFromCopy(vol Volume, srcVol Volume, copySnapshots bool, allowInconsistent bool, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// CreateVolumeFromMigration creates a volume being sent via a migration.
func (d *ploop) CreateVolumeFromMigration(vol Volume, conn io.ReadWriteCloser, volTargetArgs migration.VolumeTargetArgs, preFiller *VolumeFiller, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// RefreshVolume provides same-pool volume and specific snapshots syncing functionality.
func (d *ploop) RefreshVolume(vol Volume, srcVol Volume, srcSnapshots []Volume, allowInconsistent bool, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// UpdateVolume applies config changes to the volume.
func (d *ploop) UpdateVolume(vol Volume, changedConfig map[string]string) error {
	d.PrintTrace("", 1)

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
	d.PrintTrace("", 1)

	return 0, nil
}

// SetVolumeQuota applies a size limit on volume.
func (d *ploop) SetVolumeQuota(vol Volume, size string, allowUnsafeResize bool, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// GetVolumeDiskPath returns the location of a disk volume.
func (d *ploop) GetVolumeDiskPath(vol Volume) (string, error) {
	d.PrintTrace("", 1)

	return "", nil
}

// ListVolumes returns a list of volumes in storage pool.
func (d *ploop) ListVolumes() ([]Volume, error) {
	d.PrintTrace("", 1)

	return nil, nil
}

//TODO - think about counter, fail mount - revert back counter

// MountVolume simulates mounting a volume.
func (d *ploop) MountVolume(vol Volume, op *operations.Operation) error {

	d.PrintTrace(": "+vol.name+"; ["+vol.MountPath()+"]", 3)

	unlock, err := vol.MountLock()
	if err != nil {
		return err
	}

	defer unlock()

	// Don't attempt to modify the permission of an existing custom volume root.
	// A user inside the instance may have modified this and we don't want to reset it on restart.
	if !util.PathExists(vol.MountPath()) || vol.volType != VolumeTypeCustom {
		err := vol.EnsureMountPath()
		if err != nil {
			return err
		}
	}

	disk, res := vzgoploop.Open(vol.MountPath() + "/" + defaultDescriptor)
	if res.Status != vzgoploop.VZP_SUCCESS {
		return fmt.Errorf("VZ Ploop: Can't open disk: %s \n", res.Msg)
	}

	//TODO - think about it, maybe unit test will be enough
	status, res := disk.IsMounted()
	if res.Status != vzgoploop.VZP_SUCCESS {
		d.logger.Warn("VZ Ploop: Can't get mount disk status after mount", logger.Ctx{"msg": res.Msg})
		return nil
	}

	if status {
		count := vol.MountRefCountIncrement()
		d.logger.Debug("VZ Ploop: MountVolume - already mounted", logger.Ctx{"counter": count})
		return nil

	}

	mp := vzgoploop.VZP_MountParam{Target: vol.MountPath() + "/rootfs"}

	device, res := disk.MountImage(&mp)
	if res.Status != vzgoploop.VZP_SUCCESS {
		d.logger.Warn("VZ Ploop: Can't mount image Mount", logger.Ctx{"msg": res.Msg})
		return nil //TODO already mounted check
	}

	count := vol.MountRefCountIncrement() // From here on it is up to caller to call UnmountVolume() when done.
	d.logger.Debug("VZ Ploop: MountVolume", logger.Ctx{"counter": count})

	disk.Close()
	d.logger.Info("VZ Ploop: MountVolume - Done", logger.Ctx{"device": device})

	return nil
}

// UnmountVolume simulates unmounting a volume. As dir driver doesn't have volumes to unmount it
// returns false indicating the volume was already unmounted.
func (d *ploop) UnmountVolume(vol Volume, keepBlockDev bool, op *operations.Operation) (bool, error) {

	d.PrintTrace(": "+vol.name+"; ["+vol.MountPath()+"]", 3)

	unlock, err := vol.MountLock()
	if err != nil {
		return false, err
	}

	defer unlock()

	refCount := vol.MountRefCountDecrement()
	if refCount > 0 {
		d.logger.Info("VZ Ploop: Skipping unmount as in use", logger.Ctx{"volName": vol.name, "refCount": refCount})
		return false, ErrInUse
	}

	disk, res := vzgoploop.Open(vol.MountPath() + "/" + defaultDescriptor)

	res = disk.UmountImage()
	if res.Status != vzgoploop.VZP_SUCCESS {
		d.logger.Warn("VZ Ploop: Can't umount image", logger.Ctx{"msg": res.Msg})
	}

	status, res := disk.IsMounted()
	if res.Status != vzgoploop.VZP_SUCCESS {
		d.logger.Warn("VZ Ploop: Can't get mount disk status after umount", logger.Ctx{"msg": res.Msg})
	}

	if status {
		d.logger.Warn("VZ Ploop: Disk is unexpected mounted", logger.Ctx{"volume": vol.name})
	}

	disk.Close()

	return false, nil
}

// RenameVolume renames a volume and its snapshots.
func (d *ploop) RenameVolume(vol Volume, newVolName string, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// MigrateVolume sends a volume for migration.
func (d *ploop) MigrateVolume(vol Volume, conn io.ReadWriteCloser, volSrcArgs *migration.VolumeSourceArgs, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// BackupVolume copies a volume (and optionally its snapshots) to a specified target path.
// This driver does not support optimized backups.
func (d *ploop) BackupVolume(vol Volume, tarWriter *instancewriter.InstanceTarWriter, optimized bool, snapshots []string, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// CreateVolumeSnapshot creates a snapshot of a volume.
func (d *ploop) CreateVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// DeleteVolumeSnapshot removes a snapshot from the storage device. The volName and snapshotName
// must be bare names and should not be in the format "volume/snapshot".
func (d *ploop) DeleteVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// MountVolumeSnapshot sets up a read-only mount on top of the snapshot to avoid accidental modifications.
func (d *ploop) MountVolumeSnapshot(snapVol Volume, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// UnmountVolumeSnapshot removes the read-only mount placed on top of a snapshot.
func (d *ploop) UnmountVolumeSnapshot(snapVol Volume, op *operations.Operation) (bool, error) {
	d.PrintTrace("", 1)

	return true, nil
}

// VolumeSnapshots returns a list of snapshots for the volume (in no particular order).
func (d *ploop) VolumeSnapshots(vol Volume, op *operations.Operation) ([]string, error) {
	d.PrintTrace("", 1)

	return nil, nil
}

// RestoreVolume restores a volume from a snapshot.
func (d *ploop) RestoreVolume(vol Volume, snapshotName string, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}

// RenameVolumeSnapshot renames a volume snapshot.
func (d *ploop) RenameVolumeSnapshot(snapVol Volume, newSnapshotName string, op *operations.Operation) error {
	d.PrintTrace("", 1)

	return nil
}
