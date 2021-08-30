package mysql

import (
	"github.com/google/wire"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// Provider
var Provider = wire.NewSet(NewConfig, NewSqlBuilder)

// NewConfig
func NewConfig(v *viper.Viper) (*Config, error) {
	var err error
	o := new(Config)
	if err = v.UnmarshalKey("mysql", o); err != nil {
		return nil, errors.Wrap(err, "unmarshal app option error")
	}

	if o.RDBs == nil || len(o.RDBs) == 0 {
		o.RDBs = []Source{
			{
				Host: o.WDB.Host,
				User: o.WDB.User,
				Pass: o.WDB.Pass,
			},
		}
	}

	return o, err
}