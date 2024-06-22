package log

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/meoying/dbproxy/internal/driver/mysql/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestConnectorWrapper_Connect(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockConnector := mocks.NewMockConnector(ctrl)
		mockConn := mocks.NewMockConn(ctrl)
		mockConnector.EXPECT().Connect(gomock.Any()).Return(mockConn, nil).Times(1)
		mockDriver := mocks.NewMockDriver(ctrl)

		var connector driver.Connector = mockConnector
		var d driver.Driver = mockDriver
		var logger Logger = newMockInfoLogger(ctrl)
		var r driver.Connector = &connectorWrapper{connector: connector, driver: d, logger: logger}
		conn, err := r.Connect(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, conn)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockConnector := mocks.NewMockConnector(ctrl)
		expectedError := errors.New("mock Connect error")
		mockConnector.EXPECT().Connect(gomock.Any()).Return(nil, expectedError).Times(1)
		mockDriver := mocks.NewMockDriver(ctrl)

		var connector driver.Connector = mockConnector
		var d driver.Driver = mockDriver
		var logger Logger = newMockErrorLogger(ctrl)
		var r driver.Connector = &connectorWrapper{connector: connector, driver: d, logger: logger}
		conn, err := r.Connect(context.Background())
		assert.Error(t, err)
		assert.Nil(t, conn)
	})
}

func TestConnectorWrapper_Driver(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConnector := mocks.NewMockConnector(ctrl)
	mockDriver := mocks.NewMockDriver(ctrl)

	var connector driver.Connector = mockConnector
	var d driver.Driver = mockDriver
	var logger Logger = newMockInfoLogger(ctrl)
	var r driver.Connector = &connectorWrapper{connector: connector, driver: d, logger: logger}
	wrappedConnector := r
	assert.Equal(t, mockDriver, wrappedConnector.Driver())
}
