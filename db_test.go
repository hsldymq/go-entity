package entity

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatement(t *testing.T) {
	t.Run("select", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		_, err := selectStatement(&GenernalEntity{}, md, "mysql")
		require.NoError(t, err)

		_, err = selectStatement(&GenernalEntity{}, md, "postgres")
		require.NoError(t, err)
	})

	t.Run("insert", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		_, err := insertStatement(&GenernalEntity{}, md, "mysql")
		require.Error(t, err) // MySQL不支持returning

		_, err = insertStatement(&GenernalEntity{}, md, "postgres")
		require.NoError(t, err)
	})

	t.Run("update", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		_, err := updateStatement(&GenernalEntity{}, md, "mysql")
		require.Error(t, err) // MySQL不支持returning

		_, err = updateStatement(&GenernalEntity{}, md, "postgres")
		require.NoError(t, err)
	})

	t.Run("delete", func(t *testing.T) {
		md, _ := NewMetadata(&GenernalEntity{})

		_, err := deleteStatement(&GenernalEntity{}, md, "mysql")
		require.NoError(t, err)

		_, err = deleteStatement(&GenernalEntity{}, md, "postgres")
		require.NoError(t, err)
	})
}
