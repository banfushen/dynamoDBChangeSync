package utils

import (
	"os"
	"path/filepath"

	LOG "github.com/alecthomas/log4go"
	"github.com/nightlyone/lockfile"
)

func WritePid(id string) (err error) {
	var lock lockfile.Lockfile
	lock, err = lockfile.New(id)
	if err != nil {
		return err
	}
	if err = lock.TryLock(); err != nil {
		return err
	}

	return nil
}

func WritePidById(id string, path string) error {
	var dir string
	var err error
	if path == "" {
		if dir, err = os.Getwd(); err != nil {
			return err
		}
	} else {
		dir = path
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			os.Mkdir(dir, os.ModePerm)
		}
	}

	if dir, err = filepath.Abs(dir); err != nil {
		return err
	}

	pidfile := filepath.Join(dir, id) + ".pid"
	if err := WritePid(pidfile); err != nil {
		return err
	}
	return nil
}

func Welcome() {
	welcome :=
		`______________________________
\                                  \           _         ______ |
 \                                  \        /   \___-=O'/|O'/__|
  \  DynamoDBChangeSync, here we go !!   \_______\          / | /    )
  /                                 /        '/-==__ _/__|/__=-|  -GM
 /                                 /         *             \ | |
/                                 /                        (o)
----------------------------------
`
	startMsg := "if you have any problem, call bfs"
	LOG.Warn("\n%s%s\n\n", welcome, startMsg)
}
