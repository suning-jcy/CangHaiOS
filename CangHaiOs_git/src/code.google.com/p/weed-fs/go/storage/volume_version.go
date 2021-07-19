package storage

import ()

type Version uint8

const (
	Version1       = Version(1)
	Version2       = Version(2)
	Version3       = Version(3)
	Version4       = Version(4)
	CurrentVersion = Version4
)
