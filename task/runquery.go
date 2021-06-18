package task

import (
	_ "github.com/denisenkom/go-mssqldb"
	args "jyv.com/goarchive/args"
	con "jyv.com/goarchive/connection"
	"jyv.com/goarchive/msql"
)

func Runquery(inifile *args.IniFile) {
	ctx := new(con.Connection)
	ctx.Driver = inifile.Driver
	ctx.ConnectionString = inifile.ConnectionString
	ctx.Name = inifile.Name
	wg.Add(1)
	go msql.QuerySaveExcel(&wg, ctx, inifile.Name, inifile.Query, inifile.Output, nil, nil)
}
