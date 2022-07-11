package mysql

import (
	"github.com/google/wire"
)

// Provider
var Provider = wire.NewSet(NewConfig, NewSqlBuilder)
