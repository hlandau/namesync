package server

import "github.com/hlandau/ncdns/namecoin"
import "github.com/hlandau/ncdns/ncdomain"
import "github.com/hlandau/degoutils/log"
import "github.com/miekg/dns"
import _ "github.com/lib/pq"
import "database/sql"
import "regexp"
import "fmt"
import "strings"
import "time"
import "reflect"

const mainNetGenesisHash = "000000000062b72c5e2ceb45fbc8587e807c155b0da735e6483dfba2f0a9c770"
const genesisHeight = 0
const numPrevBlocksToKeep = 50

var re_validName = regexp.MustCompilePOSIX(`^d/(xn--)?([a-z0-9]*-)*[a-z0-9]+$`)

const maxNameLength = 65

var errInvalidName = fmt.Errorf("invalid name")

func unused(x interface{}) {}

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
	DatabaseURL         string `default:"" usage:"Database URL or other connection string (postgres: dbname=... user=... password=...)"`
	Notify              bool   `default:"false" usage:"Whether to notify (for supported databases only)"`
	NotifyName          string `default:"namecoin_updated" usage:"Event name to use for notification"`
	NSECMode            string `default:"auto" usage:"NSEC mode to use (nsec3narrow, nsec3 or nsec)"`
	StatusUpdateFunc    func(status string)
}

// Prepare statements
type prepareds struct {
	GetDomainID         *sql.Stmt `SELECT id FROM namecoin_domains WHERE name=$1 LIMIT 1`
	InsertDomain        *sql.Stmt `INSERT INTO namecoin_domains (name, last_height, value) VALUES ($1,$2,$3) RETURNING id`
	UpdateDomain        *sql.Stmt `UPDATE namecoin_domains SET last_height=$1, value=$2 WHERE id=$3`
	DeleteDomainRecords *sql.Stmt `DELETE FROM records WHERE namecoin_domain_id=$1`
	UpdateState         *sql.Stmt `UPDATE namecoin_state SET cur_block_hash=$1, cur_block_height=$2`
	InsertPrevBlock     *sql.Stmt `INSERT INTO namecoin_prevblock (block_hash, block_height) VALUES ($1,$2) RETURNING id`
	TruncatePrevBlock   *sql.Stmt `DELETE FROM namecoin_prevblock WHERE id < $1`
	InsertRecord        *sql.Stmt `INSERT INTO records (domain_id,ttl,prio,namecoin_domain_id,name,type,content,auth,ordername) VALUES (1,600,$1,$2,$3,$4,$5,$6,$7)`
	Rollback            *sql.Stmt `SELECT block_hash, block_height FROM namecoin_prevblock ORDER BY id DESC LIMIT 1`
	RollbackPop         *sql.Stmt `DELETE FROM namecoin_prevblock WHERE id IN (SELECT id FROM namecoin_prevblock ORDER BY id DESC LIMIT 1)`
	RollbackNewer       *sql.Stmt `SELECT id, name FROM namecoin_domains WHERE last_height > $1`
	DeleteName          *sql.Stmt `DELETE FROM namecoin_domains WHERE id=$1`
	Notify              *sql.Stmt `SELECT pg_notify('namecoin_updated', $1)`
}

func (p *prepareds) Prepare(db *sql.DB) error {
	t := reflect.TypeOf(p).Elem()
	v := reflect.Indirect(reflect.ValueOf(p))
	nf := t.NumField()
	for i := 0; i < nf; i++ {
		f := t.Field(i)
		if f.Tag == "" {
			continue
		}
		pr, err := db.Prepare(string(f.Tag))
		if err != nil {
			fmt.Printf("error while preparing field %d", i+1)
			return err
		}
		fv := v.Field(i)
		fv.Set(reflect.ValueOf(pr))
	}

	return nil
}

func (p *prepareds) Close() error {
	t := reflect.TypeOf(p).Elem()
	v := reflect.Indirect(reflect.ValueOf(p))
	nf := t.NumField()
	for i := 0; i < nf; i++ {
		f := t.Field(i)
		if f.Tag == "" {
			continue
		}
		fv := v.Field(i)
		fvi := fv.Interface()
		if fvi != nil {
			if stmt, ok := fvi.(*sql.Stmt); ok {
				stmt.Close()
				fv.Set(reflect.ValueOf((*sql.Stmt)(nil)))
			}
		}
	}
	return nil
}

func (p *prepareds) Tx(tx *sql.Tx) (px *prepareds, err error) {
	px = &prepareds{}
	t := reflect.TypeOf(p).Elem()
	v := reflect.Indirect(reflect.ValueOf(p))
	v2 := reflect.Indirect(reflect.ValueOf(px))
	nf := t.NumField()
	for i := 0; i < nf; i++ {
		f := t.Field(i)
		if f.Tag == "" {
			continue
		}
		fv := v.Field(i)
		fv2 := v2.Field(i)
		fvi := fv.Interface()
		if fvi != nil {
			if stmt, ok := fvi.(*sql.Stmt); ok {
				fv2.Set(reflect.ValueOf(tx.Stmt(stmt)))
			}
		}
	}
	return
}

func Run(cfg Config, startedNotifyFunc func() error) error {
	// Set up RPC connection
	c := namecoin.Conn{cfg.NamecoinRPCUsername, cfg.NamecoinRPCPassword, cfg.NamecoinRPCAddress}

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

	if cfg.NSECMode == "" || cfg.NSECMode == "auto" {
		ns3p := ""
		err := db.QueryRow("SELECT content FROM domainmetadata WHERE kind='NSEC3PARAM' and domain_id=1 LIMIT 1").Scan(&ns3p)
		if err != nil {
			cfg.NSECMode = "nsec3narrow"
		} else {
			cfg.NSECMode = "nsec3"
		}
	}

	switch cfg.NSECMode {
	case "nsec3narrow":
	case "nsec3":
		ns3p := ""
		err := db.QueryRow("SELECT content FROM domainmetadata WHERE kind='NSEC3PARAM' AND domain_id=1 LIMIT 1").Scan(&ns3p)
		if err == sql.ErrNoRows {
			return fmt.Errorf("PowerDNS must have NSEC3PARAM option for domain set to '1 0 0 8f' when NSEC3 non-narrow mode is used")
		} else if err == nil {
			if strings.ToLower(ns3p) != "1 0 0 8f" {
				return fmt.Errorf("PowerDNS must have NSEC3PARAM option for domain set to '1 0 0 8f' when NSEC3 non-narrow mode is used")
			}
		}
	default:
		panic("unsupported NSEC mode")
	}

	// Determine current state of database
	var curBlockHash = mainNetGenesisHash
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

	pr := prepareds{}
	err = pr.Prepare(db)
	if err != nil {
		return err
	}
	defer pr.Close()

	// Main loop
	var tx *sql.Tx
	var tpr *prepareds
	lastStatusUpdateTime := time.Time{}
	diverging := false

	ensureTx := func() error {
		if tx == nil {
			var err error
			tx, err = db.Begin()
			if err != nil {
				return err
			}
			tpr, err = pr.Tx(tx)
			if err != nil {
				return err
			}
		}
		return nil
	}

	defer func() {
		if tpr != nil {
			tpr.Close()
		}
	}()

	iterStartTime := time.Now()
	prevBlockHeight := curBlockHeight

	for {
		newNow := time.Now()
		timeTaken := newNow.Sub(iterStartTime)
		blockInc := curBlockHeight - prevBlockHeight
		if blockInc == 0 {
			blockInc = 1
		}
		timePerBlock := timeTaken.Seconds() / float64(blockInc)
		log.Info(fmt.Sprintf("SYNC curHeight=%6d  %2.2fs  %6d  %7.0f blocks/s", curBlockHeight, timeTaken.Seconds(), blockInc, 1/timePerBlock))
		iterStartTime = newNow
		prevBlockHeight = curBlockHeight

		events, err := c.Sync(curBlockHash, 10000, true)
		if err == namecoin.ErrSyncNoSuchBlock {
			// Rollback mode.
			diverging = true

			err = ensureTx()
			if err != nil {
				return err
			}

			err = tpr.Rollback.QueryRow().Scan(&curBlockHash, &curBlockHeight)
			if err != nil {
				// - we have run out of prevblocks
				// - any other error
				// TODO: start again from genesis block
				return err
			}

			_, err = tpr.RollbackPop.Exec()
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
			rows, err := tpr.RollbackNewer.Query(curBlockHeight)
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
					_, err = tpr.DeleteName.Exec(domainID)
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
				// (The name from name_show might actually be newer than this height, but if so it will
				//  rapidly be fixed. This avoids the need to change the .Query() interface to provide
				//  height.)
				_, err = tpr.UpdateDomain.Exec(curBlockHeight, v, domainID)
				if err != nil {
					return err
				}

				// Delete the old records for this domain.
				_, err = tpr.DeleteDomainRecords.Exec(domainID)
				if err != nil {
					return err
				}

				// Determine the new records for this domain.
				// We don't count each domain as its own zone, so we don't add SOA
				// records and use NS records only where a delegation is requested.
				vv, err := ncdomain.ParseValue(v, nil)
				if err != nil {
					continue
				}

				rrs, err := vv.RRsRecursive(nil, dnsNameFull)
				if err != nil {
					//log.Info(fmt.Sprintf("Couldn't process domain \"%s\": %s", ev.Name, err))
					continue
				}

				// Add the new records for this domain.
				err = insertRRs(tpr.InsertRecord, rrs, domainID, cfg.NSECMode)
				if err != nil {
					return err
				}
			}

			rows.Close()
			rows = nil

			tpr.Close()
			tpr = nil

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
		doNotify := (cfg.Notify && !catchingUp)

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
				err = tpr.GetDomainID.QueryRow(ev.Name).Scan(&domainID)
				if err != nil {
					err = tpr.InsertDomain.QueryRow(ev.Name, curBlockHeight, ev.Value).Scan(&domainID)
					if err != nil {
						return err
					}
				} else {
					_, err = tpr.UpdateDomain.Exec(curBlockHeight, ev.Value, domainID)
					if err != nil {
						return err
					}
				}

				// Delete the old records for this domain.
				_, err = tpr.DeleteDomainRecords.Exec(domainID)
				if err != nil {
					return err
				}

				if doNotify {
					updatedSet[dnsNameFull] = struct{}{}
				}

				// Determine the new records for this domain.
				// We don't count each domain as its own zone, so we don't add SOA
				// records and use NS records only where a delegation is requested.
				vv, err := ncdomain.ParseValue(ev.Value, nil)
				if err != nil {
					continue
				}

				rrs, err := vv.RRsRecursive(nil, dnsNameFull)
				if err != nil {
					//log.Info(fmt.Sprintf("Couldn't process domain \"%s\": %s", ev.Name, err))
					continue
				}

				// Add the new records for this domain.
				err = insertRRs(tpr.InsertRecord, rrs, domainID, cfg.NSECMode)
				if err != nil {
					return err
				}

			case "atblock":
				_, err = tpr.UpdateState.Exec(ev.BlockHash, ev.BlockHeight)
				if err != nil {
					return err
				}

				var prevBlockID int64
				err = tpr.InsertPrevBlock.QueryRow(ev.BlockHash, ev.BlockHeight).Scan(&prevBlockID)
				if err != nil {
					return err
				}

				if prevBlockID > numPrevBlocksToKeep {
					_, err = tpr.TruncatePrevBlock.Exec(prevBlockID - numPrevBlocksToKeep)
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
							_, err = tpr.Notify.Exec(updateds)
							log.Infoe(err)
							updateds = "inv"
						}
					}

					if len(updateds) > 3 {
						_, err = tpr.Notify.Exec(updateds)
						log.Infoe(err)
					}
					// don't care about errors
				}

				// For performance, only apply the last commit in a sync batch.
				skipCommit := evIdx < (len(events) - 1)

				// Commit.
				if !skipCommit {
					tpr.Close()
					tpr = nil

					err = tx.Commit()
					if err != nil {
						return err
					}

					curBlockHash = ev.BlockHash
					curBlockHeight = ev.BlockHeight

					tx = nil
				}
			}
		}
	}
}

func insertRR(stmt *sql.Stmt, rr dns.RR, domainID int64, hasNS bool, depthBelowNS int, nsecMode string) error {
	prio := 0
	hdr := rr.Header()
	stype := dns.TypeToString[hdr.Rrtype]
	value := ""

	if stype == "" {
		panic(fmt.Sprintf("no rr type: %+v", rr))
	}

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

	case dns.TypeCNAME:
		cname := rr.(*dns.CNAME)
		value = normalizeName(cname.Target)

	case dns.TypeDNAME:
		dname := rr.(*dns.DNAME)
		value = normalizeName(dname.Target)

	case dns.TypeSRV:
		srv := rr.(*dns.SRV)
		prio = int(srv.Priority)
		value = fmt.Sprintf("%d %d %s", srv.Weight, srv.Port, normalizeName(srv.Target))

	case dns.TypeMX:
		mx := rr.(*dns.MX)
		prio = int(mx.Preference)
		value = normalizeName(mx.Mx)

	case dns.TypeTXT:
		txt := rr.(*dns.TXT)

		first := true
		for _, t := range txt.Txt {
			if !first {
				value += " "
			}
			first = false
			value += "\""
			for _, c := range t {
				if c == '"' {
					value += "\""
				} else {
					value += string(c)
				}
			}
			value += "\""
		}

	default:
		return fmt.Errorf("unsupported record type: %s", rr.Header())
	}

	auth := !hasNS || (rr.Header().Rrtype == dns.TypeDS && depthBelowNS == 0)

	ordername := sql.NullString{}
	switch nsecMode {
	case "nsec3narrow":
	case "nsec3":
		ordername.String = strings.ToLower(dns.HashName(rr.Header().Name, dns.SHA1, 0, "8F"))
		ordername.Valid = true
	case "nsec":
		panic("unsupported NSEC mode")
	default:
		panic("unsupported NSEC mode")
	}

	_, err := stmt.Exec(prio, domainID, normalizeName(hdr.Name), stype, value, auth, ordername)
	if err != nil {
		return err
	}

	return nil
}

func insertRRs(stmt *sql.Stmt, rrs []dns.RR, domainID int64, nsecMode string) error {
	nsSuffix := []string{}

	for _, rr := range rrs {
		// XXX: this currently relies on a specific ordering of RRs:
		//      a.b.c
		//      x.a.b.c
		//      1.x.a.b.c
		//      2.x.a.b.c
		//      y.a.b.c
		//      b.b.c
		//      plus the assumption that at a given owner name, NS records
		//      always come first.
		//      RRsRecursive currently gives this ordering.

		// We are no longer in the current delegation level, pop it off.
		name := normalizeName(rr.Header().Name)

		if len(nsSuffix) > 0 && !domainHasSuffix(name, nsSuffix[len(nsSuffix)-1]) {
			nsSuffix = nsSuffix[0 : len(nsSuffix)-1]
		}

		// Enter a new delegation level.
		if rr.Header().Rrtype == dns.TypeNS {
			nsSuffix = append(nsSuffix, name)
		}

		depthBelowNS := 0
		if len(nsSuffix) > 0 {
			subPart := normalizeName(name[0 : len(name)-len(nsSuffix[len(nsSuffix)-1])])
			depthBelowNS = strings.Count(subPart, ".")
			if len(subPart) > 0 {
				depthBelowNS++
			}
		}

		err := insertRR(stmt, rr, domainID, len(nsSuffix) > 0, depthBelowNS, nsecMode)
		if err != nil {
			return err
		}
	}

	return nil
}

func domainHasSuffix(x, y string) bool {
	x = normalizeName(x)
	y = normalizeName(y)
	if x == y {
		return true
	}
	return strings.HasSuffix(x, "."+y)
}

func nameIsTopLevel(n string) bool {
	n = normalizeName(n)
	return strings.Count(n, ".") == 1
}

func normalizeName(n string) string {
	if len(n) == 0 {
		return n
	}
	if n[len(n)-1] == '.' {
		return n[0 : len(n)-1]
	}
	return n
}

// © 2014 Hugo Landau <hlandau@devever.net>    GPLv3 or later
