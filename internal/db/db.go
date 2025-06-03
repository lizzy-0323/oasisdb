package db

type Config struct {
}

type DB struct {
	Config Config
}

func (db *DB) Open(config *Config) error {
	return nil
}

func init() {

}
