// Copyright (c) 2021 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package minfs

import (
	"errors"
	"net/url"
	"os"
	"path"
	"strings"
)

// Config is being used for storge of configuration items
type Config struct {
	bucket   string
	basePath string

	cache       string
	quota       int
	accountID   string
	accessKey   string
	secretKey   string
	secretToken string
	target      *url.URL
	mountpoint  string
	insecure    bool
	debug       bool

	uid  uint32
	gid  uint32
	mode os.FileMode
}

// AccessConfig - access credentials and version of `config.json`.
type AccessConfig struct {
	Version     string `json:"version"`
	AccessKey   string `json:"accessKey"`
	SecretKey   string `json:"secretKey"`
	SecretToken string `json:"secretToken"`
}

// InitMinFSConfig - Initialize MinFS configuration file.
func InitMinFSConfig() (*AccessConfig, error) {
	ac := &AccessConfig{
		Version:     "1",
		AccessKey:   os.Getenv("MINIO_ACCESS_KEY"),
		SecretKey:   os.Getenv("MINIO_SECRET_KEY"),
		SecretToken: os.Getenv("MINFS_SECRET_TOKEN"),
	}

	return ac, nil
}

// Mountpoint configures the target mountpoint
func Mountpoint(mountpoint string) func(*Config) {
	return func(cfg *Config) {
		cfg.mountpoint = mountpoint
	}
}

// Target url target option for Config
func Target(target string) func(*Config) {
	return func(cfg *Config) {
		if u, err := url.Parse(target); err == nil {
			cfg.target = u

			if len(u.Path) > 1 {
				parts := strings.Split(u.Path[1:], "/")
				if len(parts) >= 0 {
					cfg.bucket = parts[0]
				}
				if len(parts) >= 1 {
					cfg.basePath = path.Join(parts[1:]...)
				}
			}
		}
	}
}

// CacheDir - cache directory path option for Config
func CacheDir(path string) func(*Config) {
	return func(cfg *Config) {
		cfg.cache = path
	}
}

// CacheDir - cache directory path option for Config
func CacheQuota(size int) func(*Config) {
	return func(cfg *Config) {
		cfg.quota = size
	}
}

// SetGID - sets a custom gid for the mount.
func SetGID(gid uint32) func(*Config) {
	return func(cfg *Config) {
		cfg.gid = gid
	}
}

// SetUID - sets a custom uid for the mount.
func SetUID(uid uint32) func(*Config) {
	return func(cfg *Config) {
		cfg.uid = uid
	}
}

// Insecure - enable insecure mode.
func Insecure() func(*Config) {
	return func(cfg *Config) {
		cfg.insecure = true
	}
}

// Debug - enables debug logging.
func Debug() func(*Config) {
	return func(cfg *Config) {
		cfg.debug = true
	}
}

// Validates the config for sane values.
func (cfg *Config) validate() error {
	// check if mountpoint exists
	if cfg.mountpoint == "" {
		return errors.New("Mountpoint not set")
	}

	if cfg.target == nil {
		return errors.New("Target not set")
	}

	if cfg.bucket == "" {
		return errors.New("Bucket not set")
	}

	return nil
}
