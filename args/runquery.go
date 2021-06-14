package args

import (
	_ "github.com/denisenkom/go-mssqldb"
	con "jyv.com/goarchive/connection"
	"jyv.com/goarchive/msql"
)

func Runquery(inifile *IniFile) {
	ctx := new(con.Connection)
	ctx.Driver = inifile.Driver
	ctx.ConnectionString = inifile.ConnectionString
	ctx.Name = inifile.Name
	go msql.QuerySaveExcel(ctx, inifile.Name, inifile.Query, inifile.Output)
}
