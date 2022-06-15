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
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/minio/minfs/meta"
	minio "github.com/minio/minio-go/v7"
)

// File implements both Node and Handle for the hello file.
type File struct {
	mfs *MinFS

	dir *Dir

	Path string

	Inode uint64

	Mode os.FileMode

	Size uint64
	ETag string

	Atime time.Time
	Mtime time.Time

	UID uint32
	GID uint32

	// OS X only
	Bkuptime time.Time
	Chgtime  time.Time
	Crtime   time.Time
	Flags    uint32 // see chflags(2)

	Hash []byte
}

func (f *File) store(tx *meta.Tx) error {
	b := f.bucket(tx)
	fmt.Printf("Storing %v at %s as %T\n", f, path.Base(f.Path), f)
	return b.Put(path.Base(f.Path), f)
}

// Attr - attr file context.
func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	*a = fuse.Attr{
		Inode:  f.Inode,
		Size:   f.Size,
		Atime:  f.Atime,
		Mtime:  f.Mtime,
		Ctime:  f.Chgtime,
		Crtime: f.Crtime,
		Mode:   f.Mode,
		Uid:    f.UID,
		Gid:    f.GID,
		Flags:  f.Flags,
	}

	return nil
}

// Setattr - set attribute.
func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// update cache with new attributes
	return f.mfs.db.Update(func(tx *meta.Tx) error {
		if req.Valid.Mode() {
			f.Mode = req.Mode
		}

		if req.Valid.Uid() {
			f.UID = req.Uid
		}

		if req.Valid.Gid() {
			f.GID = req.Gid
		}

		if req.Valid.Size() {
			f.Size = req.Size
		}

		if req.Valid.Atime() {
			f.Atime = req.Atime
		}

		if req.Valid.Mtime() {
			f.Mtime = req.Mtime
		}

		if req.Valid.Crtime() {
			f.Crtime = req.Crtime
		}

		if req.Valid.Chgtime() {
			f.Chgtime = req.Chgtime
		}

		if req.Valid.Bkuptime() {
			f.Bkuptime = req.Bkuptime
		}

		if req.Valid.Flags() {
			f.Flags = req.Flags
		}

		return f.store(tx)
	})
}

// FullPath will return the full path
func (f *File) FullPath() string {
	return path.Join(f.dir.FullPath(), f.Path)
}

func (f *File) ObjectPath() string {
	return strings.Replace(f.FullPath(), f.Bucket()+"/", "", 1)
}

func (f *File) Bucket() string {
	return strings.Split(f.FullPath(), "/")[0] // Bucket will always be given as first part of remote path
}

// Saves a new file at cached path and fetches the object based on
// the incoming fuse request.
func (f *File) cacheSave(ctx context.Context, path string, req *fuse.OpenRequest, api *minio.Client) error {

	// TODO: This should block if another instance of this function is running for the same path

	if _, err := os.Stat(path); err == nil {
		currentTime := time.Now().Local()
		err = os.Chtimes(path, currentTime, currentTime)
		return err
	}

	if req.Flags&fuse.OpenTruncate == fuse.OpenTruncate {
		f.Size = 0
		return nil
	}

	// FGetObject faster, safer implimentation for large files
	// mfs.log.Println("FGetObject():", ctx, f.mfs.config.bucket, f.RemotePath(), path, minio.GetObjectOptions{})
	err := api.FGetObject(ctx, f.Bucket(), f.ObjectPath(), path, minio.GetObjectOptions{})
	if err != nil {
		if meta.IsNoSuchObject(err) {
			return fuse.ENOENT
		}
		return err
	}

	cachedFile, err := os.Stat(path)

	if err != nil {
		return err
	}

	// update actual file size
	f.Size = uint64(cachedFile.Size())

	// Success.
	return nil
}

// Generates a cache path based on the minio MD5 checksum
func (f *File) cacheAllocate(ctx context.Context, api *minio.Client) (string, error) {

	object, err := api.StatObject(ctx, f.Bucket(), f.ObjectPath(), minio.GetObjectOptions{})

	if err != nil {
		if meta.IsNoSuchObject(err) {
			return "", fuse.ENOENT
		}
		return "", err
	}

	// Success.
	cachePath := path.Join(f.mfs.config.cache, object.Key+"-"+object.ETag+".fcache")

	return cachePath, err
}

// Open return a file handle of the opened file
func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {

	start := time.Now()

	resp.Flags |= fuse.OpenDirectIO

	api, err := f.mfs.getApi(req.Uid)
	if err != nil {
		fmt.Println("Some error with getApi()")
		return nil, err
	}

	cachePath, err := f.cacheAllocate(ctx, api)
	if err != nil {
		fmt.Println("Some error with cacheAllocate()")
		return nil, err
	}

	// Once we know the cache path (RESOURCE), we lock it down until the Open request is fully served
	unlock := f.mfs.km.Lock(cachePath)
	defer unlock()

	err = f.cacheSave(ctx, cachePath, req, api)
	if err != nil {
		f.mfs.log.Println("Some error with cacheSave", err)
		return nil, err
	}

	fh, err := f.mfs.Acquire(f, cachePath)
	if err != nil {
		f.mfs.log.Println("Some error with Acquire", err)
		return nil, err
	}

	fh.cachePath = cachePath

	fh.File, err = os.OpenFile(fh.cachePath, int(req.Flags), f.mfs.config.mode)
	if err != nil {
		f.mfs.log.Println("Some error with OpenFile", err)
		return nil, err
	}

	resp.Handle = fuse.HandleID(fh.handle)

	f.mfs.log.Println("Serving FH request [", fh.handle, "], acquired file lock on: ", f.FullPath(), " cache resource @", cachePath, "took", time.Since(start))

	return fh, nil
}

func (f *File) bucket(tx *meta.Tx) *meta.Bucket {
	b := f.dir.bucket(tx)
	return b
}

// Getattr returns the file attributes
func (f *File) Getattr(ctx context.Context, req *fuse.GetattrRequest, resp *fuse.GetattrResponse) error {
	resp.Attr = fuse.Attr{
		Inode:  f.Inode,
		Size:   f.Size,
		Atime:  f.Atime,
		Mtime:  f.Mtime,
		Ctime:  f.Chgtime,
		Crtime: f.Crtime,
		Mode:   f.Mode,
		Uid:    f.UID,
		Gid:    f.GID,
		Flags:  f.Flags,
	}

	return nil
}

// Dirent returns the File object as a fuse.Dirent
func (f File) Dirent() fuse.Dirent {
	return fuse.Dirent{
		Inode: f.Inode, Name: f.Path, Type: fuse.DT_File,
	}
}

// Dirent will return the fuse Dirent for current dir
func (f File) Dirpath() string {
	return f.Path
}

func (f *File) delete(tx *meta.Tx) error {
	// purge from cache
	b := f.bucket(tx)
	return b.Delete(f.Path)
}
