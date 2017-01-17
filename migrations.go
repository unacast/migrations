package migrations

import (
	"database/sql"
	"sort"
	"time"

	"fmt"
)

// New creates the migrator context
func New(db *sql.DB) *Migrator {
	migrator := &Migrator{connection: db}
	if !migrator.migrationsTableExists() {
		migrator.createMigrationTable()
	}
	return migrator
}

// Migrator keeps the state of the migration. This structure is
// used to run migrations
type Migrator struct {
	connection *sql.DB
}

// GetFiles defines the function interface for providing
// filepaths to the migrator. The user is expected to implement this function.
type GetFiles func() []string

// GetContent defines the function interface for providing content from a file.
// The expected input is a filepath and the output is the content of that file.
// The user is expected to implement this function.
type GetContent func(string) string

type migration struct {
	file      string
	timestamp time.Time
}

const migrationTableName = "__migrations"
const createMigrationTableSQL = `
    CREATE TABLE pollo.__migrations (
    file VARCHAR(255) NOT NULL,
    timestamp DATETIME NOT NULL,
    PRIMARY KEY (file));
`

// RunMigration executes the migration
// - Get candidate files
// - Get already migrated files
// - Execute all the files that hasn't been migrated
// - Update migration table with result
func (migrator *Migrator) RunMigration(getFiles GetFiles, getContent GetContent) {
	startTime := time.Now().UTC()
	logDebug("Starting migration: ", startTime)
	fileNames := getFiles()
	sort.Strings(fileNames)
	existingMigrations := migrator.getExistingMigrations()

	existingMigrationMap := make(map[string]migration)

	for _, m := range existingMigrations {
		existingMigrationMap[m.file] = m
	}

	tx, err := migrator.connection.Begin()
	if err != nil {
		panic("Failed to create transaction for migration: " + err.Error())
	}
	newMigrations := make([]migration, 0, 10)

	logDebug("All migrations:", fileNames)
	for _, f := range fileNames {
		if _, ok := existingMigrationMap[f]; !ok {
			sqlContent := getContent(f)

			logDebug("Running migration: ", f)
			logDebug("With content: ", sqlContent)

			timestamp := time.Now().UTC()
			_, err2 := migrator.connection.Exec(sqlContent)
			if err2 != nil {
				logError("Failed to execute migration: ", f, err)
				tx.Rollback()
				panic(err)
			}
			mig := migration{file: f, timestamp: timestamp}
			err = migrator.addMigration(migration{file: f, timestamp: timestamp})
			if err != nil {
				logError("Failed to update migration table: ", err)
				tx.Rollback()
				panic(err)
			}
			newMigrations = append(newMigrations, mig)
		}
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	endTime := time.Now().UTC()
	duration := endTime.Sub(startTime)
	logDebug("Migration done: ", endTime)
	logDebug("Migration duration: ", duration)
}

func (migrator *Migrator) migrationsTableExists() bool {
	rows, err := migrator.connection.Query("SHOW TABLES")
	if err != nil {
		logError("Couldn't query for tables", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			logError("Failed to read file item row: ", err)
		}
		if tableName == migrationTableName {
			return true
		}
	}

	return false
}

func (migrator *Migrator) createMigrationTable() {
	_, err := migrator.connection.Exec(createMigrationTableSQL)
	if err != nil {
		logError("Failed to create migration table: " + err.Error())
	}
}

func (migrator *Migrator) addMigration(migration migration) error {
	stmt, err := migrator.connection.Prepare(fmt.Sprintf("INSERT INTO %s(file, timestamp) VALUES(?,?)", migrationTableName))
	if err != nil {
		return err
	}
	_, err = stmt.Exec(migration.file, migration.timestamp)
	if err != nil {
		return err
	}
	return nil
}

func (migrator *Migrator) getExistingMigrations() []migration {
	rows, err := migrator.connection.Query(fmt.Sprintf("SELECT file, timestamp FROM %s", migrationTableName))
	if err != nil {
		panic("Failed to create migration select statement: " + err.Error())
	}
	defer rows.Close()
	migrations := make([]migration, 0, 10)
	for rows.Next() {
		var (
			file      string
			timestamp time.Time
		)
		err = rows.Scan(&file, &timestamp)
		if err != nil {
			panic("Failed to scan migration row: " + err.Error())
		}
		migrations = append(migrations, migration{file: file, timestamp: timestamp})
	}
	return migrations
}
