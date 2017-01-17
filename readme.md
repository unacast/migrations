# migrations

Simple, roll-forward only, migrations package for SQL written in go.

## Example with assets

```
func runMigrations() {
	db, _ := connectToDb() // should return *sql.DB
	getFiles := func() []string {
		files, _ := assets.AssetDir("migrations/sql")
		return files
	}
	getContent := func(file string) string {
		bytes, _ := assets.Asset(fmt.Sprintf("migrations/sql/%s", file))
		return string(bytes)
	}
	migrator := migrations.New(db)

	migrator.Migrate(getFiles, getContent)
	db.Close()
}
```

## TODO

- [ ] Write a better README.MD

