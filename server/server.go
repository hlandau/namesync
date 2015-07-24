package server

import "github.com/hlandau/ncbtcjsontypes"
import "github.com/hlandau/ncdns/namecoin"
import "github.com/hlandau/ncdns/ncdomain"
import "github.com/hlandau/ncdns/util"
import "github.com/hlandau/xlog"
import "github.com/miekg/dns"
import "fmt"
import "time"
import "strings"
import _ "github.com/lib/pq"
import "database/sql"

const mainNetGenesisHash = "000000000062b72c5e2ceb45fbc8587e807c155b0da735e6483dfba2f0a9c770"
const genesisHeight = 0
const numPrevBlocksToKeep = 50

var log, Log = xlog.New("namesync")

func init() {
	Log.SetSeverity(xlog.SevNotice)
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
	StartedNotifyFunc   func() error
	Suffix              string `default:"bit" usage:"Zone name (no trailing dot, e.g. 'bit')"`
}

type Server struct {
	cfg            Config
	nc             namecoin.Conn
	db             *sql.DB
	pr             prepareds
	curBlockHash   string
	curBlockHeight int64
	nsec3param     dns.NSEC3PARAM

	tx                    *sql.Tx
	tpr                   *prepareds
	maxBlockHeightAtStart int64
	lastStatusUpdateTime  time.Time
	updatedSet            map[string]struct{}
	domainID              int64
	diverging             bool
}

func Run(cfg Config) error {
	s := &Server{
		cfg:        cfg,
		updatedSet: map[string]struct{}{},
	}

	if s.cfg.Suffix == "" {
		s.cfg.Suffix = "bit"
	}

	s.nc = namecoin.Conn{
		Username: s.cfg.NamecoinRPCUsername,
		Password: s.cfg.NamecoinRPCPassword,
		Server:   s.cfg.NamecoinRPCAddress,
	}

	maxBlockHeightAtStart, err := s.nc.CurHeight()
	if err != nil {
		return err
	}

	s.maxBlockHeightAtStart = int64(maxBlockHeightAtStart)

	db, err := sql.Open(s.cfg.DatabaseType, s.cfg.DatabaseURL)
	if err != nil {
		return err
	}

	s.db = db
	s.db.SetMaxIdleConns(1)
	s.db.SetMaxOpenConns(2)

	if s.cfg.DatabaseType != "postgres" {
		s.cfg.Notify = false
	}

	err = s.determineDomainID()
	if err != nil {
		return err
	}

	err = s.determineNSECMode()
	if err != nil {
		return err
	}

	s.curBlockHash, s.curBlockHeight, err = s.determineState()
	if err != nil {
		return err
	}

	log.Infof("Starting with initial state height=%d, block=%s", s.curBlockHeight, s.curBlockHash)

	err = s.pr.Prepare(s.db)
	if err != nil {
		return err
	}

	defer s.pr.Close()
	defer func() {
		if s.tpr != nil {
			s.tpr.Close()
		}
	}()

	err = s.loop()
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) determineDomainID() error {
	err := s.db.QueryRow("SELECT id FROM domains WHERE name=$1 LIMIT 1", s.cfg.Suffix).Scan(&s.domainID)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) determineNSECMode() error {
	s.autodetectNSEC3()

	switch s.cfg.NSECMode {
	case "nsec3":
		ns3p, err := s.getNSEC3Param()
		if err == sql.ErrNoRows {
			return fmt.Errorf("PowerDNS database must have NSEC3PARAM option for domain when NSEC3 non-narrow mode is used")
		} else if err != nil {
			return err
		}

		nsr, err := dns.NewRR(". IN NSEC3PARAM " + ns3p)
		if err != nil {
			return err
		}

		s.nsec3param = *(nsr.(*dns.NSEC3PARAM))

	case "nsec3narrow":
		// Don't need to do anything.

	default:
		return fmt.Errorf("unknown NSEC mode: %v", s.cfg.NSECMode)
	}

	return nil
}

func (s *Server) autodetectNSEC3() {
	if s.cfg.NSECMode == "" || s.cfg.NSECMode == "auto" {
		_, err := s.getNSEC3Param()
		if err != nil {
			s.cfg.NSECMode = "nsec3narrow"
		} else {
			s.cfg.NSECMode = "nsec3"
		}
	}
}

func (s *Server) getNSEC3Param() (string, error) {
	var ns3p string
	err := s.db.QueryRow("SELECT content FROM domainmetadata WHERE kind='NSEC3PARAM' AND domain_id=$1 LIMIT 1", s.domainID).Scan(&ns3p)
	if err != nil {
		return "", err
	}

	return ns3p, nil
}

func (s *Server) determineState() (curBlockHash string, curBlockHeight int64, err error) {
	err = s.db.QueryRow("SELECT cur_block_hash, cur_block_height FROM namecoin_state LIMIT 1").
		Scan(&curBlockHash, &curBlockHeight)

	if err == sql.ErrNoRows {
		// Database not primed, insert genesis block.
		curBlockHash = mainNetGenesisHash
		curBlockHeight = genesisHeight

		_, err = s.db.Exec("INSERT INTO namecoin_state (cur_block_hash, cur_block_height) VALUES ($1,$2)",
			curBlockHash, curBlockHeight)
	}

	return
}

func (s *Server) ensureTx() error {
	if s.tx == nil {
		var err error
		s.tx, err = s.db.Begin()
		if err != nil {
			return err
		}
		s.tpr, err = s.pr.Tx(s.tx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) commit() error {
	if s.tx == nil {
		return nil
	}

	s.tpr.Close()
	s.tpr = nil

	s.tx.Commit()
	s.tx = nil

	return nil
}

func (s *Server) loop() error {
	for {
		events, err := s.nc.Sync(s.curBlockHash, 10000, true)
		if err == namecoin.ErrSyncNoSuchBlock {
			// Finish any open transaction.
			err = s.commit()
			if err != nil {
				return err
			}

			// Find a common point of divergence.
			err = s.rollback()
			if err != nil {
				return err
			}

			// Now try again with the new prevblock.
			continue
		}

		err = s.handleDivergence()
		if err != nil {
			return err
		}

		if s.cfg.StartedNotifyFunc != nil {
			// Defer the call to startedNotifyFunc until after the call to name_sync so that we error out
			// before declaring ourselves as started in the event the Namecoin RPC server doesn't support
			// name_sync.
			err = s.cfg.StartedNotifyFunc()
			if err != nil {
				return err
			}

			s.cfg.StartedNotifyFunc = nil
		}

		s.logProgress()

		if s.cfg.StatusUpdateFunc != nil {
			now := time.Now()
			if now.Sub(s.lastStatusUpdateTime) > 1*time.Minute {
				s.lastStatusUpdateTime = now
				status := "ok"
				if s.isCatchingUp() {
					status = fmt.Sprintf("ok (catching up: %d/%d)", s.curBlockHeight, s.maxBlockHeightAtStart)
				}
				s.cfg.StatusUpdateFunc(status)
			}
		}

		err = s.processEvents(events)
		if err != nil {
			return err
		}
	}
}

func (s *Server) logProgress() {
	log.Noticef("SYNC curHeight=%d", s.curBlockHeight)
}

func (s *Server) rollback() error {
	// Rollback mode.
	s.diverging = true

	err := s.ensureTx()
	if err != nil {
		return err
	}

	err = s.tpr.Rollback.QueryRow().Scan(&s.curBlockHash, &s.curBlockHeight)
	if err != nil {
		// - We have run out of prevblocks.
		// - Any other error.
		// TODO: start again from genesis block.
		return err
	}

	_, err = s.tpr.RollbackPop.Exec()
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDivergence() error {
	if !s.diverging {
		return nil
	}

	// We have finished the rollback process and found the divergence point.
	rows, err := s.tpr.RollbackNewer.Query(s.curBlockHeight)
	if err != nil {
		return err
	}

	defer rows.Close()

	// Get the correct values at this point for all newer names.
	for rows.Next() {
		var domainID int64
		var nName string // name is in namecoin format, e.g. "d/example"

		err = rows.Scan(&domainID, &nName)
		if err != nil {
			return err
		}

		v, err := s.nc.Query(nName)
		if err != nil {
			// Assume the name does not exist anymore, delete it.
			_, err = s.tpr.DeleteName.Exec(domainID)
			if err != nil {
				return err
			}

			// TODO: dependent names
			continue
		}

		// Fabricate an update event.
		ev := ncbtcjsontypes.NameSyncEvent{
			Type:  "update", // XXX
			Name:  nName,
			Value: v,
		}

		err = s.processUpdate(ev)
		if err != nil {
			return err
		}
	}

	err = s.commit()
	if err != nil {
		return err
	}

	s.diverging = false
	return nil
}

func (s *Server) processEvents(events []ncbtcjsontypes.NameSyncEvent) error {
	for evIdx, ev := range events {
		err := s.ensureTx()
		if err != nil {
			return err
		}

		switch ev.Type {
		case "update", "firstupdate":
			err = s.processUpdate(ev)
		case "atblock":
			// For performance, only apply the last commit in a sync batch.
			skipCommit := evIdx < (len(events) - 1)
			err = s.processAtblock(ev, !skipCommit)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) processUpdate(ev ncbtcjsontypes.NameSyncEvent) error {
	log.Debugf("update %#v", ev.Name)

	// Determine whether the name maps to a valid DNS name.
	basename, err := util.NamecoinKeyToBasename(ev.Name)
	if err != nil {
		return nil
	}

	// Update domain name and get ID.
	domainID, err := s.setDomainValue(ev.Name, ev.Value, s.curBlockHeight)
	if err != nil {
		return fmt.Errorf("can't set domain value: %v", err)
	}

	// Delete the old records for this domain.
	_, err = s.tpr.DeleteDomainRecords.Exec(domainID)
	if err != nil {
		return fmt.Errorf("can't delete domain records: %v", err)
	}

	_, err = s.tpr.DeleteDomainDeps.Exec(domainID)
	if err != nil {
		return fmt.Errorf("can't delete domain deps: %v", err)
	}

	dnsname := basename + "." + s.cfg.Suffix
	s.setNotification(dnsname)

	notedDeps := map[string]struct{}{}

	resolveFunc := func(name string) (string, error) {
		var value string

		qerr := s.tpr.GetDomainValue.QueryRow(name).Scan(&value)

		if _, ok := notedDeps[name]; !ok {
			deferred := (qerr != nil)

			_, err := s.tpr.InsertDomainDep.Exec(domainID, name, deferred)
			if err != nil {
				return "", err
			}

			notedDeps[name] = struct{}{}
		}

		if qerr != nil {
			return "", qerr
		}

		return value, nil
	}

	// Determine the new records for this domain.
	// We don't count each domain as its own zone, so we don't add SOA
	// records and use NS records only where a delegation is requested.
	vv := ncdomain.ParseValue(ev.Name, ev.Value, resolveFunc, nil)
	if vv == nil {
		return nil
	}

	rrs, err := vv.RRsRecursive(nil, dnsname, dnsname)
	if err != nil {
		return err
	}

	err = s.insertRRs(rrs, domainID)
	if err != nil {
		return err
	}

	// Update dependencies.
	rows, err := s.tpr.GetDomainDeps.Query(ev.Name)
	if err != nil {
		return fmt.Errorf("can't get domain deps: %v", err)
	}
	defer rows.Close()

	depUpdates := []ncbtcjsontypes.NameSyncEvent{}

	for rows.Next() {
		var depName, depValue string

		err := rows.Scan(&depName, &depValue)
		if err != nil {
			return err
		}

		// Fabricate an update event.
		depUpdates = append(depUpdates, ncbtcjsontypes.NameSyncEvent{
			Type:  "update",
			Name:  depName,
			Value: depValue,
		})
	}

	rows.Close()

	for _, psuedoEvent := range depUpdates {
		err = s.processUpdate(psuedoEvent)
		if err != nil {
			return err
		}
	}

	return nil
}

// Determine the namecoin domain ID, creating the record if it doesn't
// already exist. Update the block height and value.
func (s *Server) setDomainValue(name, value string, curBlockHeight int64) (domainID int64, err error) {
	err = s.tpr.GetDomainID.QueryRow(name).Scan(&domainID)
	if err != nil {
		err = s.tpr.InsertDomain.QueryRow(name, curBlockHeight, value).Scan(&domainID)
	} else {
		_, err = s.tpr.UpdateDomain.Exec(curBlockHeight, value, domainID)
	}
	return
}

func (s *Server) processAtblock(ev ncbtcjsontypes.NameSyncEvent, commit bool) error {
	err := s.appendCurrentState(ev.BlockHash, int64(ev.BlockHeight))
	if err != nil {
		return err
	}

	err = s.sendNotifications()
	if err != nil {
		return err
	}

	if commit {
		err = s.commit()
		if err != nil {
			return err
		}
	}

	s.curBlockHash = ev.BlockHash
	s.curBlockHeight = int64(ev.BlockHeight)
	log.Infof("atblock %d", s.curBlockHeight)

	return nil
}

func (s *Server) appendCurrentState(blockHash string, blockHeight int64) error {
	_, err := s.tpr.UpdateState.Exec(blockHash, blockHeight)
	if err != nil {
		return err
	}

	err = s.addPrevBlock(blockHash, blockHeight)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) addPrevBlock(blockHash string, blockHeight int64) error {
	var prevBlockID int64
	err := s.tpr.InsertPrevBlock.QueryRow(blockHash, blockHeight).Scan(&prevBlockID)
	if err != nil {
		return err
	}

	if prevBlockID > numPrevBlocksToKeep {
		_, err = s.tpr.TruncatePrevBlock.Exec(prevBlockID - numPrevBlocksToKeep)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) sendNotifications() error {
	defer s.clearNotifications()

	if !s.shouldNotify() {
		return nil
	}

	updateds := "inv"
	for k := range s.updatedSet {
		updateds += " " + k
		if len(updateds) > 6000 {
			_, err := s.tpr.Notify.Exec(updateds)
			if err != nil {
				return err
			}
			updateds = "inv"
		}
	}

	if len(updateds) > 3 {
		_, err := s.tpr.Notify.Exec(updateds)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) shouldNotify() bool {
	return s.cfg.Notify && !s.isCatchingUp()
}

func (s *Server) isCatchingUp() bool {
	return s.curBlockHeight < s.maxBlockHeightAtStart-2
}

func (s *Server) clearNotifications() {
	s.updatedSet = map[string]struct{}{}
}

func (s *Server) setNotification(name string) {
	s.updatedSet[name] = struct{}{}
}

func (s *Server) insertRRs(rrs []dns.RR, domainID int64) error {
	nsSuffix := []string{}

	for _, rr := range rrs {
		name := normalizeName(rr.Header().Name)

		if len(nsSuffix) > 0 && !domainHasSuffix(name, nsSuffix[len(nsSuffix)-1]) {
			nsSuffix = nsSuffix[0 : len(nsSuffix)-1]
		}

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

		err := s.insertRR(rr, domainID, len(nsSuffix) > 0, depthBelowNS)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) insertRR(rr dns.RR, domainID int64, hasNS bool, depthBelowNS int) error {
	stype, value, prio, err := convertRRData(rr)
	if err != nil {
		return err
	}

	hdr := rr.Header()

	auth := !hasNS || (hdr.Rrtype == dns.TypeDS && depthBelowNS == 0)

	orderName, err := s.determineOrderName(hdr.Name)
	if err != nil {
		return err
	}

	_, err = s.tpr.InsertRecord.Exec(prio, domainID, normalizeName(hdr.Name), stype, value, auth, orderName)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) determineOrderName(name string) (orderName sql.NullString, err error) {
	switch s.cfg.NSECMode {
	case "nsec3narrow":
		// no orderName
	case "nsec3":
		orderName.String = strings.ToLower(dns.HashName(name, s.nsec3param.Hash, s.nsec3param.Iterations, s.nsec3param.Salt))
		orderName.Valid = true
	}

	return
}

func convertRRData(rr dns.RR) (stype, value string, prio int, err error) {
	hdr := rr.Header()
	stype = dns.TypeToString[hdr.Rrtype]

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
			value += `"`
			for _, c := range t {
				if c == '"' {
					value += `"`
				} else {
					value += string(c)
				}
			}
			value += `"`
		}

	default:
		err = fmt.Errorf("unsupported record type: %s", rr.Header())
		return
	}

	return
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
