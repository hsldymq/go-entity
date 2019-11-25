package entity

import (
	"context"
	"database/sql"
	"strings"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"    // 载入 goqu mysql驱动
	_ "github.com/doug-martin/goqu/v9/dialect/postgres" // 载入 goqu postgres驱动
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"  // 载入 goqu sqlite3驱动
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

var (
	selectStatements = map[string]string{}
	insertStatements = map[string]string{}
	updateStatements = map[string]string{}
	deleteStatements = map[string]string{}
)

// DB 数据库接口
// sqlx.DB 和 sqlx.Tx 公共方法
type DB interface {
	sqlx.Queryer
	sqlx.QueryerContext
	sqlx.Execer
	sqlx.ExecerContext
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	DriverName() string
	Rebind(string) string
	BindNamed(string, interface{}) (string, []interface{}, error)
}

func doLoad(ctx context.Context, ent Entity, db DB) error {
	md, err := getMetadata(ent)
	if err != nil {
		return err
	}

	stmt, ok := selectStatements[md.ID]
	if !ok {
		stmt, err = selectStatement(ent, md, db.DriverName())
		if err != nil {
			return err
		}
		selectStatements[md.ID] = stmt
	}

	rows, err := sqlx.NamedQueryContext(ctx, db, stmt, ent)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return errors.WithStack(sql.ErrNoRows)
	}

	if err := rows.StructScan(ent); err != nil {
		return errors.WithStack(err)
	}

	return errors.WithStack(rows.Err())
}

func doInsert(ctx context.Context, ent Entity, db DB) (int64, error) {
	md, err := getMetadata(ent)
	if err != nil {
		return 0, err
	}

	stmt, ok := insertStatements[md.ID]
	if !ok {
		stmt, err = insertStatement(ent, md, db.DriverName())
		if err != nil {
			return 0, err
		}
		insertStatements[md.ID] = stmt
	}

	if md.hasReturningInsert {
		rows, err := sqlx.NamedQueryContext(ctx, db, stmt, ent)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		defer rows.Close()

		if !rows.Next() {
			return 0, errors.WithStack(sql.ErrNoRows)
		}

		if err := rows.StructScan(ent); err != nil {
			return 0, errors.WithStack(err)
		}

		return 0, errors.WithStack(rows.Err())
	}

	result, err := db.NamedExecContext(ctx, stmt, ent)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	// postgres不支持LastInsertId特性
	if isPostgres(db.DriverName()) {
		return 0, nil
	}

	lastID, err := result.LastInsertId()
	return lastID, errors.WithStack(err)
}

func doUpdate(ctx context.Context, ent Entity, db DB) error {
	md, err := getMetadata(ent)
	if err != nil {
		return err
	}

	stmt, ok := updateStatements[md.ID]
	if !ok {
		stmt, err = updateStatement(ent, md, db.DriverName())
		if err != nil {
			return err
		}
		updateStatements[md.ID] = stmt
	}

	if md.hasReturningUpdate {
		rows, err := sqlx.NamedQueryContext(ctx, db, stmt, ent)
		if err != nil {
			return errors.WithStack(err)
		}
		defer rows.Close()

		if !rows.Next() {
			return errors.WithStack(sql.ErrNoRows)
		}

		if err := rows.StructScan(ent); err != nil {
			return errors.WithStack(err)
		}

		return errors.WithStack(rows.Err())
	}

	result, err := db.NamedExecContext(ctx, stmt, ent)
	if err != nil {
		return errors.WithStack(err)
	}

	if n, err := result.RowsAffected(); err != nil {
		return errors.WithStack(err)
	} else if n == 0 {
		return errors.WithStack(sql.ErrNoRows)
	}

	return nil
}

func doDelete(ctx context.Context, ent Entity, db DB) error {
	md, err := getMetadata(ent)
	if err != nil {
		return errors.Wrap(err, "delete entity")
	}

	stmt, ok := deleteStatements[md.ID]
	if !ok {
		stmt, err = deleteStatement(ent, md, db.DriverName())
		if err != nil {
			return err
		}
		deleteStatements[md.ID] = stmt
	}

	_, err = db.NamedExecContext(ctx, stmt, ent)
	return errors.Wrapf(err, "delete entity %s", md.ID)
}

func selectStatement(ent Entity, md *Metadata, driver string) (string, error) {
	columns := make([]interface{}, 0, len(md.Columns))
	for _, col := range md.Columns {
		columns = append(columns, col.DBField)
	}

	stmt := goqu.Dialect(driverName(driver)).
		From(md.TableName).
		Prepared(false).
		Select(columns...).
		Limit(1)

	where := goqu.Ex{}
	for _, col := range md.PrimaryKeys {
		where[col.DBField] = goqu.L(":" + col.DBField)
	}
	stmt = stmt.Where(where)
	sqlStr, _, err := stmt.ToSQL()

	return sqlStr, errors.WithStack(err)
}

func insertStatement(ent Entity, md *Metadata, driver string) (string, error) {
	var columns []interface{}
	var returning []interface{}
	var valuePlaceholder []interface{}

	for _, col := range md.Columns {
		if col.ReturningInsert {
			returning = append(returning, col.DBField)
		} else if !col.AutoIncrement {
			columns = append(columns, col.DBField)
			valuePlaceholder = append(valuePlaceholder, goqu.L(":"+col.DBField))
		}
	}

	stmt := goqu.Dialect(driverName(driver)).
		Insert(md.TableName).
		Prepared(false).
		Cols(columns...).
		Vals(valuePlaceholder)

	if len(returning) > 0 {
		stmt = stmt.Returning(returning...)
	}

	sqlStr, _, err := stmt.ToSQL()

	return sqlStr, errors.WithStack(err)
}

func updateStatement(ent Entity, md *Metadata, driver string) (string, error) {
	var returning []interface{}

	stmt := goqu.Dialect(driverName(driver)).
		Update(md.TableName).
		Prepared(false)

	set := goqu.Ex{}
	for _, col := range md.Columns {
		if col.ReturningUpdate {
			returning = append(returning, col.DBField)
		} else if !col.RefuseUpdate {
			set[col.DBField] = goqu.L(":" + col.DBField)
		}
	}
	stmt = stmt.Set(set)

	where := goqu.Ex{}
	for _, col := range md.PrimaryKeys {
		where[col.DBField] = goqu.L(":" + col.DBField)
	}
	stmt = stmt.Where(where)

	if len(returning) > 0 {
		stmt = stmt.Returning(returning...)
	}

	sqlStr, _, err := stmt.ToSQL()

	return sqlStr, errors.WithStack(err)
}

func deleteStatement(ent Entity, md *Metadata, driver string) (string, error) {
	stmt := goqu.Dialect(driverName(driver)).
		Delete(md.TableName).
		Prepared(false)

	where := goqu.Ex{}
	for _, col := range md.PrimaryKeys {
		where[col.DBField] = goqu.L(":" + col.DBField)
	}
	stmt = stmt.Where(where)
	sqlStr, _, err := stmt.ToSQL()

	return sqlStr, errors.WithStack(err)
}

// isConflictError 给定的错误是否是唯一性约束错误
func isConflictError(driver string, err error) bool {
	s := errors.Cause(err).Error()
	if isPostgres(driver) {
		return strings.Contains(s, "duplicate key value violates unique constraint")
	} else if isMySQL(driver) {
		return strings.Contains(s, "Duplicate entry")
	} else if isSQLite3(driver) {
		return strings.Contains(s, "UNIQUE constraint failed")
	}
	return false
}

// isPostgres 给定driver是否是postgres
func isPostgres(driver string) bool {
	return driverName(driver) == "postgres"
}

// isPostgres 给定driver是否是mysql
func isMySQL(driver string) bool {
	return driverName(driver) == "mysql"
}

// isPostgres 给定driver是否是sqlite3
func isSQLite3(driver string) bool {
	return driverName(driver) == "sqlite3"
}

// driverName 驱动名称
func driverName(driver string) string {
	if driver == "pgx" {
		return "postgres"
	}

	return driver
}
