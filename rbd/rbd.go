package rbd

import (
	"fmt"
	//"math/rand"
	//"path/filepath"
	"strconv"
	"strings"
	"sync"
	"os/exec"

	"github.com/Sirupsen/logrus"
	//"github.com/rancher/convoy/util"
	"github.com/ceph/go-ceph/rados"

	. "github.com/rancher/convoy/convoydriver"
)

const (
	DRIVER_NAME        = "rbd"
	DRIVER_CONFIG_FILE = "rbd.cfg"

	VOLUME_CFG_PREFIX = "volume_"
	DRIVER_CFG_PREFIX = DRIVER_NAME + "_"
	CFG_POSTFIX       = ".json"

	SNAPSHOT_PATH = "snapshots"

	MOUNTS_DIR = "mounts"

	CEPH_SERVERS             = "ceph.servers"
	CEPH_SECRET              = "ceph.secret"
	CEPH_DEFAULT_VOLUME_POOL = "ceph.pool"
	CEPH_DEFAULT_VOLUME_SIZE = "ceph.defaultvolumesize"
	CEPH_DEFAULT_USER = "ceph.user"
	DEFAULT_VOLUME_SIZE           = "1G"
)

type volume struct {
	name   string // RBD Image name
	device string // local host kernel device (e.g. /dev/rbd1)
	locker string // track the lock name
	fstype string
	pool   string
	//clientid string
}

type cephRBDVolumeDriver struct {
	// - using default ceph cluster name ("ceph")
	// - using default ceph config (/etc/ceph/<cluster>.conf)
	//
	// TODO: when starting, what if there are mounts already for RBD devices?
	// do we ingest them as our own or ... currently fails if locked
	//
	// TODO: what about before dying - do we unmap and unmount everything? docker could still be running ...
	// TODO: use a chan as semaphore instead of mutex in driver?

	name         string             // unique name for plugin
	cluster      string             // ceph cluster to use (default: ceph)
	user         string             // ceph user to use (default: admin)
	defaultPool  string             // default ceph pool to use (default: rbd)
	root         string             // scratch dir for mounts for this plugin
	config       string             // ceph config file to read
	volumes      map[string]*volume // track locally mounted volumes
	m            *sync.Mutex        // mutex to guard operations that change volume maps or use conn
	conn         *rados.Conn        // keep an open connection
	defaultIoctx *rados.IOContext   // context for default pool
}

type Device struct {
	Root              string
	Servers           []string
	DefaultVolumePool string
	DefaultVolumeSize int64
}

type RBDVolume struct {
	Name       string // volume name in fact
	MountPoint string
	Servers    []string

	configPath string
}

type Driver struct {
	mutex      *sync.RWMutex
	rVolumes   map[string]*RBDVolume
	User       string
	Secret     string
	Device
}


func init() {
	fmt.Printf("Test for debug.\n")
	logrus.Debugf("test debug.\n")
	Register(DRIVER_NAME, Init)
}

func (d *Driver) Name() string {
	return DRIVER_NAME
}

func (d *Driver) Info() (map[string]string, error) {
	return map[string]string{
		"Root":              d.Root,
		"RBDServers":  fmt.Sprintf("%v", d.Servers),
		"RBDUser":     d.User,
		"RBDSecret":   d.Secret,
		"DefaultVolumePool": d.DefaultVolumePool,
		"DefaultVolumeSize": strconv.FormatInt(d.DefaultVolumeSize, 10),
	}, nil
}

func Init(root string, config map[string]string) (ConvoyDriver, error) {
	//servers := []string{"172.16.70.16", "172.16.70.19", "172.16.70.21"}
	logrus.Debugf("config:%v", config)
	serverList := config[CEPH_SERVERS]
	servers := strings.Split(serverList, ",")
	
	user := config[CEPH_DEFAULT_USER]
	secret := config[CEPH_SECRET]
	
	defaultVolumePool := "rancher"
	
	dev := &Device{
		Root:              root,
		Servers:           servers,
		DefaultVolumePool: defaultVolumePool,
		DefaultVolumeSize: 10,
	}
	
	d := &Driver{
		mutex:      &sync.RWMutex{},
		rVolumes: map[string]*RBDVolume{},
		User:       user,
		Secret:     secret,
		Device:     *dev,
	}
	rVolume := &RBDVolume{
		Name:       dev.DefaultVolumePool,
		Servers:    dev.Servers,
		configPath: d.Root,
	}
	
	d.rVolumes[d.DefaultVolumePool] = rVolume
	//if err := util.ObjectSave(dev); err != nil {
	//	return nil, err
	//}
	return d, nil
}

func (d *Driver) VolumeOps() (VolumeOperations, error) {
	return nil, fmt.Errorf("Doesn't support volume operations")
}


func (d *Driver) SnapshotOps() (SnapshotOperations, error) {
	return nil, fmt.Errorf("Doesn't support snapshot operations")
}

func (d *Driver) BackupOps() (BackupOperations, error) {
	return nil, fmt.Errorf("Doesn't support backup operations")
}

func isDebugEnabled() bool {
	return true
}

// sh is a simple os.exec Command tool, returns trimmed string output
func sh(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	if isDebugEnabled() {
		logrus.Debug("DEBUG: sh CMD: %q", cmd)
	}
	// TODO: capture and output STDERR to logfile?
	out, err := cmd.Output()
	return strings.Trim(string(out), " \n"), err
}