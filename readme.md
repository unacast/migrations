# migrations

Simple, roll-forward only, migrations package for MySQL written in go.

## Getting started

### Install

To install `migrations` simply pull this library using `go get`.

```sh
go get github.com/unacast/migrations
```

### Setup

To setup and use `migrations` you need to implement two functions `GetFiles` and `GetContent`.

`GetFiles` should return a list of files with their fullpath. This can be from anywhere as long as it's supported `GetContent`.

`GetContent` takes a filepath and returns it's content.

#### Example using assets

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
