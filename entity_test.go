package entity

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetadata(t *testing.T) {
	_, err := NewMetadata(&EmptyEntity{})
	require.Error(t, err)

	_, err = NewMetadata(&NoPrimaryKeyEntity{})
	require.Error(t, err)

	md, err := NewMetadata(&GenernalEntity{})
	require.NoError(t, err)

	require.Equal(t, 2, len(md.PrimaryKeys))
	require.Equal(t, 5, len(md.Columns))
	require.Equal(t, (&GenernalEntity{}).TableName(), md.TableName)
}

func TestColumns(t *testing.T) {
	cases := map[string]struct {
		primaryKey      bool
		refuseUpdate    bool
		autoIncrement   bool
		returningInsert bool
		returningUpdate bool
	}{
		"id": {
			primaryKey:    true,
			autoIncrement: true,
			refuseUpdate:  true,
		},
		"id2": {
			primaryKey:   true,
			refuseUpdate: true,
		},
		"name": {},
		"create_at": {
			refuseUpdate:    true,
			returningInsert: true,
		},
		"version": {
			returningInsert: true,
			returningUpdate: true,
			refuseUpdate:    true,
		},
	}

	columns, err := getColumns(&GenernalEntity{})
	require.NoError(t, err)

	for _, col := range columns {
		expected := cases[col.DBField]

		require.Equal(t, expected.primaryKey, col.PrimaryKey)
		require.Equal(t, expected.refuseUpdate, col.RefuseUpdate)
		require.Equal(t, expected.autoIncrement, col.AutoIncrement)
		require.Equal(t, expected.returningInsert, col.ReturningInsert)
		require.Equal(t, expected.returningUpdate, col.ReturningUpdate)
	}

	_, err = getColumns(GenernalEntity{})
	require.Error(t, err)
}

type GenernalEntity struct {
	ID             int       `db:"id" entity:"primaryKey,autoIncrement"`
	ID2            int       `db:"id2" entity:"primaryKey"`
	Name           string    `db:"name"`
	CreateAt       time.Time `db:"create_at" entity:"refuseUpdate,returningInsert"`
	Version        int       `db:"version" entity:"returning"`
	Deprecated     bool      `db:"deprecated" entity:"deprecated"`
	ExplicitIgnore bool      `db:"-"`
	ImplicitIgnore bool
}

func (ge GenernalEntity) TableName() string {
	return "genernal"
}

func (ge GenernalEntity) OnEntityEvent(ctx context.Context, ev Event) error {
	return nil
}

type EmptyEntity struct {
	ID   int
	Name string
}

func (ee EmptyEntity) TableName() string {
	return "emtpy"
}

func (ee *EmptyEntity) OnEntityEvent(ctx context.Context, ev Event) error {
	return nil
}

type NoPrimaryKeyEntity struct {
	ID   int    `db:"int"`
	Name string `db:"name"`
}

func (npe NoPrimaryKeyEntity) TableName() string {
	return "no_primary_key"
}

func (npe *NoPrimaryKeyEntity) OnEntityEvent(ctx context.Context, ev Event) error {
	return nil
}
