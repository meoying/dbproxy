package log

import (
	"errors"
	"testing"

	logmocks "github.com/meoying/dbproxy/internal/protocol/mysql/driver/log/mocks"
	"github.com/meoying/dbproxy/internal/protocol/mysql/driver/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDriver_Open(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		name := "dsn"
		mockDriver := mocks.NewMockDriver(ctrl)
		mockDriver.EXPECT().Open(name).Return(nil, nil)
		wrappedDriver := newDriver(mockDriver, newMockInfoLogger(ctrl))

		c, err := wrappedDriver.Open(name)

		require.NoError(t, err)
		require.NotZero(t, c)
	})
	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		name := "dsn"
		mockDriver := mocks.NewMockDriver(ctrl)
		mockDriver.EXPECT().Open(name).Return(nil, errors.New("mock Open Error"))
		wrappedDriver := newDriver(mockDriver, newMockErrorLogger(ctrl))

		c, err := wrappedDriver.Open(name)
		require.Error(t, err)
		require.Zero(t, c)
	})
}

func TestDriver_OpenConnector(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		name := "dsn"
		mockDriverContext := mocks.NewMockDriver(ctrl)
		mockDriverContext.EXPECT().OpenConnector(name).Return(nil, nil)
		wrappedDriver := newDriver(mockDriverContext, newMockInfoLogger(ctrl))

		c, err := wrappedDriver.OpenConnector(name)

		require.NoError(t, err)
		require.NotZero(t, c)
	})
	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		name := "dsn"
		mockDriverContext := mocks.NewMockDriver(ctrl)
		mockDriverContext.EXPECT().OpenConnector(name).Return(nil, errors.New("mock OpenConnector Error"))
		wrappedDriver := newDriver(mockDriverContext, newMockErrorLogger(ctrl))

		c, err := wrappedDriver.OpenConnector(name)
		require.Error(t, err)
		require.Zero(t, c)
	})
}

func newMockInfoLogger(ctrl *gomock.Controller) *logmocks.Mocklogger {
	logger := logmocks.NewMocklogger(ctrl)
	logger.EXPECT().Info(gomock.Any(), gomock.Any()).Times(1)
	return logger
}

func newMockErrorLogger(ctrl *gomock.Controller) *logmocks.Mocklogger {
	logger := logmocks.NewMocklogger(ctrl)
	logger.EXPECT().Error(gomock.Any(), gomock.Any()).Times(1)
	return logger
}
