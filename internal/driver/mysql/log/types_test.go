package log

import (
	"log/slog"
	"os"
	"testing"

	"github.com/meoying/dbproxy/internal/driver/mysql/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestNewConnector(t *testing.T) {

	t.Run("默认log", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		d := mocks.NewMockDriver(ctrl)
		dsn := "valid_dsn"
		d.EXPECT().OpenConnector(dsn).Return(mocks.NewMockConnector(ctrl), nil)

		conn, err := NewConnector(d, dsn)

		require.NoError(t, err)
		assert.NotZero(t, conn)
	})

	t.Run("自定义log", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		d := mocks.NewMockDriver(ctrl)
		dsn := "valid_dsn"
		d.EXPECT().OpenConnector(dsn).Return(mocks.NewMockConnector(ctrl), nil)

		customLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
		conn, err := NewConnector(d, dsn, WithLogger(customLogger))

		require.NoError(t, err)
		assert.NotZero(t, conn)
	})
}
