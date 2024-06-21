package log

import (
	"errors"
	"testing"

	"github.com/meoying/dbproxy/internal/driver/mysql/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestConn_New(t *testing.T) {

}

func TestConn_Connect(t *testing.T) {

}

// Conn 测试用例
func (s *driverTestSuite) TestConn_Prepare_Logf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	query := "SELECT * FROM `users`"
	conn := mocks.NewMockConn(ctrl)
	conn.EXPECT().Prepare(query).Return(&stmtWrapper{}, nil).Times(1)
	wrappedConn := &connWrapper{conn: conn, logger: newMockLogLogger(ctrl)}

	stmt, err := wrappedConn.Prepare(query)
	assert.NoError(t, err)
	assert.NotZero(t, stmt)
}

func (s *driverTestSuite) TestConn_Prepare_Errorf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	query := "SELECT * FROM `users`"
	mockConn := mocks.NewMockConn(ctrl)
	mockConn.EXPECT().Prepare(query).Return(nil, errors.New("mock Prepare error")).Times(1)
	wrappedConn := &connWrapper{conn: mockConn, logger: newMockErrorLogger(ctrl)}

	stmt, err := wrappedConn.Prepare(query)
	assert.Error(t, err)
	assert.Zero(t, stmt)
}
