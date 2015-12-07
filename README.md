namesync
========

Namesync is a daemon written in Go which keeps a PostgreSQL database
using a PowerDNS-compatible schema updated with the contents of
the Namecoin zone file. It relies on a Namecoin full node.

Namesync requires a patch to the full node in order to add the
`name_sync` RPC command, which enables live updates using long
polling. Currently this patch is available only for namecore.
It is available in the doc/ directory.

Type `make` to fetch the source code and build automatically.

Licenced under the GPLv3+.
