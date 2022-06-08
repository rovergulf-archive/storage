package storage

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// Object is a generic representation of a storage object
type Object struct {
	Meta         Metadata
	Path         string
	Data         []byte
	LastModified time.Time
}

// Metadata represents the meta information of the object
// includes object name , object version , etc...
type Metadata struct {
	Name    string
	Version string
}

// ObjectSliceDiff provides information on what has changed since last calling ListObjects
type ObjectSliceDiff struct {
	Change  bool
	Removed []Object
	Added   []Object
	Updated []Object
}

// HasExtension determines whether or not an object contains a file extension
func (object Object) HasExtension(extension string) bool {
	return filepath.Ext(object.Path) == fmt.Sprintf(".%s", extension)
}

// GetObjectSliceDiff takes two objects slices and returns an ObjectSliceDiff
func GetObjectSliceDiff(prev []Object, curr []Object, timestampTolerance time.Duration) ObjectSliceDiff {
	var diff ObjectSliceDiff
	pos := make(map[string]Object)
	cos := make(map[string]Object)
	for _, o := range prev {
		pos[o.Path] = o
	}
	for _, o := range curr {
		cos[o.Path] = o
	}
	// for every object in the previous slice, if it exists in the current slice, check if it is *considered as* updated;
	// otherwise, mark it as removed
	for _, p := range prev {
		if c, found := cos[p.Path]; found {
			if c.LastModified.Sub(p.LastModified) > timestampTolerance {
				diff.Updated = append(diff.Updated, c)
			}
		} else {
			diff.Removed = append(diff.Removed, p)
		}
	}
	// for every object in the current slice, if it does not exist in the previous slice, mark it as added
	for _, c := range curr {
		if _, found := pos[c.Path]; !found {
			diff.Added = append(diff.Added, c)
		}
	}
	// if any object is marked as removed or added or updated, set change to true
	diff.Change = len(diff.Removed)+len(diff.Added)+len(diff.Updated) > 0
	return diff
}

func cleanPrefix(prefix string) string {
	return strings.Trim(prefix, "/")
}

func removePrefixFromObjectPath(prefix string, path string) string {
	if prefix == "" {
		return path
	}
	path = strings.Replace(path, fmt.Sprintf("%s/", prefix), "", 1)
	return path
}

func objectPathIsInvalid(path string) bool {
	return strings.Contains(path, "/") || path == ""
}
