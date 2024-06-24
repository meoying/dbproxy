package log

import (
	"database/sql/driver"
	"errors"
	"testing"

	driver2 "github.com/meoying/dbproxy/internal/protocol/mysql/driver"
	logmocks "github.com/meoying/dbproxy/internal/protocol/mysql/driver/log/mocks"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

//go:generate mockgen -source=./driver_test.go -destination=mocks/driver.mock.go -package=logmocks -typed Driver
type Driver interface {
	driver.Driver
	driver2.Driver
}

func TestDriver_OpenConnector(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		name := "dsn"
		mockDriverContext := logmocks.NewMockDriver(ctrl)
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
		mockDriverContext := logmocks.NewMockDriver(ctrl)
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
