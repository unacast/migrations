package migrations

import (
	"database/sql"
	"log"
	"os"
	"sort"
	"time"

	"fmt"
)

type Migrator struct {
	connection *sql.DB
}

type GetFiles func() []string
type GetContent func(string) string

type migration struct {
	file      string
	timestamp time.Time
}

var logError = log.New(os.Stderr, "[ERROR] ", log.LstdFlags)
var logDebug = log.New(os.Stdout, "[DEBUG]", log.LstdFlags)

const migrationTableName = "__migrations"
const createMigrationTableSQL = `
    CREATE TABLE pollo.__migrations (
    file VARCHAR(255) NOT NULL,
    timestamp DATETIME NOT NULL,
    PRIMARY KEY (file));
`

// GetMigrator creates the migrator context
func GetMigrator(db *sql.DB) *Migrator {
	migrator := &Migrator{connection: db}
	if !migrator.migrationsTableExists() {
		migrator.createMigrationTable()
	}
	return migrator
}

// RunMigration executes the migration
// - Get candidate files
// - Get already migrated files
// - Execute all the files that hasn't been migrated
// - Update migration table with result
func (migrator *Migrator) RunMigration(getFiles GetFiles, getContent GetContent) {
	startTime := time.Now().UTC()
	logDebug.Println("Starting migration: ", startTime)
	fileNames := getFiles()
	sort.Strings(fileNames)
	existingMigrations := migrator.getExistingMigrations()

	existingMigrationMap := make(map[string]migration)

	for _, m := range existingMigrations {
		existingMigrationMap[m.file] = m
	}

	tx, err := migrator.connection.Begin()
	if err != nil {
		logError.Panic("Failed to create transaction for migration: ", err)
	}
	newMigrations := make([]migration, 0, 10)
	logDebug.Println("All migrations:", fileNames)
	for _, f := range fileNames {
		if _, ok := existingMigrationMap[f]; !ok {
			sqlContent := getContent(f)

			logDebug.Println("Running migration: ", f)
			logDebug.Println("With content: ", sqlContent)

			timestamp := time.Now().UTC()
			_, err := migrator.connection.Exec(sqlContent)
			if err != nil {
				logError.Println("Failed to execute migration: ", f, err)
				tx.Rollback()
				panic(err)
			}
			mig := migration{file: f, timestamp: timestamp}
			err = migrator.addMigration(migration{file: f, timestamp: timestamp})
			if err != nil {
				logError.Println("Failed to update migration table: ", err)
				tx.Rollback()
				panic(err)
			}
			newMigrations = append(newMigrations, mig)
		}
	}
	err = tx.Commit()
	endTime := time.Now().UTC()
	duration := endTime.Sub(startTime)
	logDebug.Println("Migration done: ", endTime)
	logDebug.Println("Migration duration: ", duration)
}

func (migrator *Migrator) migrationsTableExists() bool {
	rows, err := migrator.connection.Query("SHOW TABLES")
	defer rows.Close()
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			logError.Panic("Failed to read file item row: ", err)
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
		logError.Panic("Failed to create migration table", err)
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
		logError.Panic("Failed to create migration select statement: ", err)
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
			logError.Panic("Failed to scan migration row: ", err)
		}
		migrations = append(migrations, migration{file: file, timestamp: timestamp})
	}
	return migrations
}