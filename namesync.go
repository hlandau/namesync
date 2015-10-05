package main

import "gopkg.in/hlandau/easyconfig.v1"
import "gopkg.in/hlandau/service.v2"
import "github.com/hlandau/namesync/server"

func main() {
	cfg := server.Config{}
	config := easyconfig.Configurator{
		ProgramName: "namesync",
	}
	config.ParseFatal(&cfg)

	service.Main(&service.Info{
		Name:          "namesync",
		Description:   "Namecoin to SQL Database Synchronization Daemon",
		DefaultChroot: service.EmptyChrootPath,
		RunFunc: func(smgr service.Manager) error {
			doneChan := make(chan error)

			cfg.StatusUpdateFunc = func(status string) {
				smgr.SetStatus("namesync: " + status)
			}

			cfg.StartedNotifyFunc = func() error {
				err := smgr.DropPrivileges()
				if err != nil {
					return err
				}

				smgr.SetStarted()
				return nil
			}

			cfg.StatusUpdateFunc("starting")

			go func() {
				err := server.Run(cfg)
				doneChan <- err
			}()

			select {
			case <-smgr.StopChan():
				// Stop was requested. Just return, everything in the daemon is transactional
				// so we don't need to worry about a clean shutdown.
				return nil

			case err := <-doneChan:
				// Daemon stopped spontaneously. Run() never returns nil, though.
				if err == nil {
					panic("unreachable")
				}
				return err
			}

			return nil
		},
	})
}

// © 2014 Hugo Landau <hlandau@devever.net>    GPLv3 or later
