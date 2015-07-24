package server

import "github.com/hlandau/degoutils/net/sqlprep"
import "database/sql"
import "reflect"

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

	// namecoin_deps keeps track of which names depend on which (imported names).
	InsertDomainDep  *sql.Stmt `INSERT INTO namecoin_deps (namecoin_domain_id,name,deferred) VALUES ($1,$2,$3)`
	DeleteDomainDeps *sql.Stmt `DELETE FROM namecoin_deps WHERE namecoin_domain_id=$1`
	GetDomainValue   *sql.Stmt `SELECT value FROM namecoin_domains WHERE name=$1 LIMIT 1`

	GetDomainDeps *sql.Stmt `SELECT namecoin_domains.name, value FROM namecoin_deps LEFT JOIN namecoin_domains ON (namecoin_domain_id=id) WHERE namecoin_deps.name=$1`
}

func (p *prepareds) Prepare(db *sql.DB) error {
	return sqlprep.Prepare(p, db)
}

func (p *prepareds) Close() error {
	return sqlprep.Close(p)
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
