package server
import "github.com/hlandau/ncdns/namecoin"
import "github.com/hlandau/ncdns/backend"
import "github.com/hlandau/degoutils/log"
import "github.com/miekg/dns"
import _ "github.com/lib/pq"
import "database/sql"
import "regexp"
import "fmt"
import "strings"
import "time"

const mainNetGenesisHash   = "000000000062b72c5e2ceb45fbc8587e807c155b0da735e6483dfba2f0a9c770"
const genesisHeight        = 0
const numPrevBlocksToKeep  = 50

var re_validName = regexp.MustCompilePOSIX(`^d/(xn--)?([a-z0-9]*-)*[a-z0-9]+$`)
const maxNameLength = 65
var errInvalidName = fmt.Errorf("invalid name")

func unused(x interface{}){}

func convertName(n string) (string, error) {
	if len(n) > maxNameLength || !re_validName.MatchString(n) {
		return "", errInvalidName
	}
	return n[2:], nil
}

func unconvertName(n string) string {
	return "d/" + n
}

type Config struct {
	NamecoinRPCUsername string `default:"" usage:"Namecoin RPC username"`
	NamecoinRPCPassword string `default:"" usage:"Namecoin RPC password"`
	NamecoinRPCAddress  string `default:"127.0.0.1:8332" usage:"Namecoin RPC server address"`
	DatabaseType        string `default:"postgres" usage:"Database type to use (supported values: postgres)"`
	//"postgres", "user=namecoin_dev dbname=namecoin_dev sslmode=disable password=namecoin_dev")
	DatabaseURL         string `default:"" usage:"Database URL or other connection string (postgres: dbname=... user=... password=...)"`
	Notify              bool   `default:"false" usage:"Whether to notify (for supported databases only)"`
	NotifyName          string `default:"namecoin_updated" usage:"Event name to use for notification"`
	StatusUpdateFunc    func(status string)
}

func Run(cfg Config, startedNotifyFunc func() error) error {
	// Set up RPC connection
	c := namecoin.Conn{cfg.NamecoinRPCUsername,cfg.NamecoinRPCPassword,cfg.NamecoinRPCAddress,}

	// Determine current block chain height
	maxBlockHeightAtStart, err := c.CurHeight()
	if err != nil {
		return err
	}

	// Open database
	db, err := sql.Open(cfg.DatabaseType, cfg.DatabaseURL)
	if err != nil {
		return err
	}

	defer db.Close()

	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)

	if cfg.DatabaseType != "postgres" {
		cfg.Notify = false
	}

	// Determine current state of database
	var curBlockHash   = mainNetGenesisHash
	var curBlockHeight = genesisHeight

	err = db.QueryRow("SELECT cur_block_hash, cur_block_height FROM namecoin_state LIMIT 1").Scan(&curBlockHash, &curBlockHeight)
	if err == sql.ErrNoRows {
		// Database not primed, insert genesis block
		_, err = db.Exec("INSERT INTO namecoin_state (cur_block_hash, cur_block_height) VALUES ($1,$2)", curBlockHash, curBlockHeight)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// Prepare statements
	prGetDomainID, err := db.Prepare("SELECT id FROM namecoin_domains WHERE name=$1 LIMIT 1")
	if err != nil { return err }
	prInsertDomain, err := db.Prepare("INSERT INTO namecoin_domains (name,last_height) VALUES ($1,$2) RETURNING id")
	if err != nil { return err }
	prUpdateDomainHeight, err := db.Prepare("UPDATE namecoin_domains SET last_height=$1 WHERE id=$2")
	if err != nil { return err }
	prDeleteDomainRecords, err := db.Prepare("DELETE FROM records WHERE namecoin_domain_id=$1")
	if err != nil { return err }
	prUpdateState, err := db.Prepare("UPDATE namecoin_state SET cur_block_hash=$1, cur_block_height=$2")
	if err != nil { return err }
	prInsertPrevBlock, err := db.Prepare("INSERT INTO namecoin_prevblock (block_hash, block_height) VALUES ($1,$2) RETURNING id")
	if err != nil { return err }
	prTruncatePrevBlock, err := db.Prepare("DELETE FROM namecoin_prevblock WHERE id < $1")
	if err != nil { return err }
	prInsertRecord, err := db.Prepare("INSERT INTO records (domain_id,ttl,prio,namecoin_domain_id,name,type,content) VALUES (1,600,$1,$2,$3,$4,$5)")
	if err != nil { return err }
	prRollback, err := db.Prepare("SELECT block_hash, block_height FROM namecoin_prevblock ORDER BY id DESC LIMIT 1")
	if err != nil { return err }
	prRollbackPop, err := db.Prepare("DELETE FROM namecoin_prevblock WHERE id IN (SELECT id FROM namecoin_prevblock ORDER BY id DESC LIMIT 1)")
	if err != nil { return err }
	prRollbackNewer, err := db.Prepare("SELECT id, name FROM namecoin_domains WHERE last_height > $1")
	if err != nil { return err }
	prDeleteName, err := db.Prepare("DELETE FROM namecoin_domains WHERE id=$1")
	if err != nil { return err }

	var prNotify *sql.Stmt
	if cfg.Notify {
		prNotify, err = db.Prepare("SELECT pg_notify('namecoin_updated', $1)")
		if err != nil {
			return err
		}
	}

	// Main loop
	var tx *sql.Tx
	lastStatusUpdateTime := time.Time{}
	diverging := false

	ensureTx := func() error {
		if tx == nil {
			var err error
			tx, err = db.Begin()
			if err != nil {
				return err
			}
		}
		return nil
	}

	for {
		log.Info(fmt.Sprintf("SYNC curHeight=%d", curBlockHeight))
		events, err := c.Sync(curBlockHash, 1000, true)
		if err == namecoin.ErrSyncNoSuchBlock {
			// Rollback mode.
			diverging = true

			err = ensureTx()
			if err != nil {
				return err
			}

			err = tx.Stmt(prRollback).QueryRow().Scan(&curBlockHash, &curBlockHeight)
			if err != nil {
				// - we have run out of prevblocks
				// - any other error
				// TODO: start again from genesis block
				return err
			}

			_, err = tx.Stmt(prRollbackPop).Exec()
			if err != nil {
				return err
			}

			// now try again with the new prevblock
			continue
		} else if err != nil {
			return err
		}

		if diverging {
			// We have finished the rollback process and found the divergence point.
			rows, err := tx.Stmt(prRollbackNewer).Query(curBlockHeight)
			if err != nil {
				return err
			}

			defer func() {
				if rows != nil {
					rows.Close()
				}
			}()

			// Get the correct values at this point for all newer names.
			for rows.Next() {
				var domainID int64
				var nName string // name is in namecoin format, e.g. "d/example"

				err = rows.Scan(&domainID, &nName)
				if err != nil {
					return err
				}

				v, err := c.Query(nName)
				if err != nil {
					// assume the name does not exist anymore, delete it
					_, err = tx.Stmt(prDeleteName).Exec(domainID)
					if err != nil {
						return err
					}
				}

				// reconvert
				dnsName, err := convertName(nName)
				if err != nil {
					return err
				}

				dnsNameFull := dnsName + ".bit"

				// Determine the namecoin domain ID, creating the record if it doesn't already exist.
				// Update the block height to the current value.
				_, err = tx.Stmt(prUpdateDomainHeight).Exec(curBlockHeight, domainID)
				if err != nil {
					return err
				}

				// Delete the old records for this domain.
				_, err = tx.Stmt(prDeleteDomainRecords).Exec(domainID)
				if err != nil {
					return err
				}

				// Determine the new records for this domain.
				// We don't count each domain as its own zone, so we don't add SOA
				// records and use NS records only where a delegation is requested.
				rrs, err := backend.Convert(dnsNameFull, v)
				if err != nil {
					//log.Info(fmt.Sprintf("Couldn't process domain \"%s\": %s", ev.Name, err))
					continue
				}

				// Add the new records for this domain.
				for _, rr := range rrs {
					err = insertRR(tx.Stmt(prInsertRecord), rr, domainID)
					if err != nil {
						return err
					}
				}
			}

			rows.Close()
			rows = nil

			err = tx.Commit()
			if err != nil {
				return err
			}

			tx = nil
			diverging = false
			// rollback complete, resume normal operation
		}

		if startedNotifyFunc != nil {
			// Defer the call to startedNotifyFunc until after the call to name_sync so that we error out before declaring
			// ourselves as started in the event the Namecoin RPC server doesn't support name_sync, which is likely to be
			// one of the most common error cases.
			err = startedNotifyFunc()
			if err != nil {
				return err
			}

			startedNotifyFunc = nil
		}

		updatedSet := map[string]struct{}{}

		catchUpThreshold := maxBlockHeightAtStart - 2
		catchingUp := curBlockHeight < catchUpThreshold
		doNotify := (prNotify != nil && !catchingUp)

		if cfg.StatusUpdateFunc != nil {
			now := time.Now()
			if now.Sub(lastStatusUpdateTime) > 1*time.Minute {
				lastStatusUpdateTime = now

				status := "ok"
				if catchingUp {
					status = fmt.Sprintf("ok (catching up: %d/%d)", curBlockHeight, maxBlockHeightAtStart)
				}
				cfg.StatusUpdateFunc(status)
			}
		}

		for evIdx, ev := range events {
			err = ensureTx()
			if err != nil {
				return err
			}

			/*if tx == nil {
				tx, err = db.Begin()
				if err != nil {
					return err
				}
			}*/

			switch ev.Type {
				case "update", "firstupdate":
					var domainID int64

					// Determine whether the name maps to a valid DNS name.
					dnsName, err := convertName(ev.Name)
					if err != nil {
						continue
					}

					dnsNameFull := dnsName + ".bit"

					// Determine the namecoin domain ID, creating the record if it doesn't already exist.
					// Update the block height to the current value.
					err = tx.Stmt(prGetDomainID).QueryRow(ev.Name).Scan(&domainID)
					if err != nil {
						err = tx.Stmt(prInsertDomain).QueryRow(ev.Name, curBlockHeight).Scan(&domainID)
						if err != nil {
							return err
						}
					} else {
						_, err = tx.Stmt(prUpdateDomainHeight).Exec(curBlockHeight, domainID)
						if err != nil {
							return err
						}
					}

					// Delete the old records for this domain.
					_, err = tx.Stmt(prDeleteDomainRecords).Exec(domainID)
					if err != nil {
						return err
					}

					if doNotify {
						updatedSet[dnsNameFull] = struct{}{}
					}

					// Determine the new records for this domain.
					// We don't count each domain as its own zone, so we don't add SOA
					// records and use NS records only where a delegation is requested.
					rrs, err := backend.Convert(dnsNameFull, ev.Value)
					if err != nil {
						//log.Info(fmt.Sprintf("Couldn't process domain \"%s\": %s", ev.Name, err))
						continue
					}

					// Add the new records for this domain.
					for _, rr := range rrs {
						err = insertRR(tx.Stmt(prInsertRecord), rr, domainID)
						if err != nil {
							return err
						}
					}

				case "atblock":
					_, err = tx.Stmt(prUpdateState).Exec(ev.BlockHash, ev.BlockHeight)
					if err != nil {
						return err
					}

					var prevBlockID int64
					err = tx.Stmt(prInsertPrevBlock).QueryRow(ev.BlockHash, ev.BlockHeight).Scan(&prevBlockID)
					if err != nil {
						return err
					}

					if prevBlockID > numPrevBlocksToKeep {
						_, err = tx.Stmt(prTruncatePrevBlock).Exec(prevBlockID - numPrevBlocksToKeep)
						if err != nil {
							return err
						}
					}

					// Notify
					if doNotify {
						updateds := "inv"
						for k := range updatedSet {
							updateds += " " + k
							if len(updateds) > 6000 {
								_, err = tx.Stmt(prNotify).Exec(updateds)
								log.Infoe(err)
								updateds = "inv"
							}
						}

						if len(updateds) > 3 {
							_, err = tx.Stmt(prNotify).Exec(updateds)
							log.Infoe(err)
						}
						// don't care about errors
					}

					// For performance, only apply the last commit in a sync batch.
					skipCommit := evIdx < (len(events)-1)

					// Commit.
					if !skipCommit {
						err = tx.Commit()
						if err != nil {
							return err
						}

						curBlockHash   = ev.BlockHash
						curBlockHeight = ev.BlockHeight

						tx = nil
					}
			}
		}
	}
}

func insertRR(stmt *sql.Stmt, rr dns.RR, domainID int64) error {
	prio := 0
	hdr := rr.Header()
	stype := dns.TypeToString[hdr.Rrtype]
	value := ""

	switch hdr.Rrtype {
		case dns.TypeA:
			value = rr.(*dns.A).A.String()

		case dns.TypeAAAA:
			value = rr.(*dns.AAAA).AAAA.String()

		case dns.TypeNS:
			value = normalizeName(rr.(*dns.NS).Ns)

		case dns.TypeDS:
			ds := rr.(*dns.DS)
			value = fmt.Sprintf("%d %d %d %s", ds.KeyTag, ds.Algorithm, ds.DigestType, strings.ToUpper(ds.Digest))

		//case dns.TypeSRV:
		//case dns.TypeTXT:
		default:
			return fmt.Errorf("unsupported record type")
	}

	_, err := stmt.Exec(prio, domainID, normalizeName(hdr.Name), stype, value)
	if err != nil {
		return err
	}

	return nil
}

func normalizeName(n string) string {
	if len(n) == 0 {
		return n
	}
	if n[len(n)-1] == '.' {
		return n[0:len(n)-1]
	}
	return n
}

// © 2014 Hugo Landau <hlandau@devever.net>    GPLv3 or later
