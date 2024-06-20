package log

import (
	"database/sql/driver"
	"errors"
	"testing"

	logmocks "github.com/meoying/dbproxy/internal/driver/mysql/log/mocks"
	"github.com/meoying/dbproxy/internal/driver/mysql/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

func TestLogDriverTestSuite(t *testing.T) {
	suite.Run(t, new(driverTestSuite))
}

type driverTestSuite struct {
	suite.Suite
}

// Driver 测试用例
func (s *driverTestSuite) TestDriver_Open_Logf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	name := "dsn"
	mockDriver := mocks.NewMockDriver(ctrl)
	mockDriver.EXPECT().Open(name).Return(nil, nil)
	wrappedDriver := newDriver(mockDriver, nil, newMockLogLogger(ctrl))

	c, err := wrappedDriver.Open(name)

	require.NoError(t, err)
	require.NotZero(t, c)
}

func newMockLogLogger(ctrl *gomock.Controller) *logmocks.MockLogger {
	logger := logmocks.NewMockLogger(ctrl)
	logger.EXPECT().Logf(gomock.Any(), gomock.Any()).Times(1)
	return logger
}

func (s *driverTestSuite) TestDriver_Open_Errorf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	name := "dsn"
	mockDriver := mocks.NewMockDriver(ctrl)
	mockDriver.EXPECT().Open(name).Return(nil, errors.New("mock Open Error"))
	wrappedDriver := newDriver(mockDriver, nil, newMockErrorLogger(ctrl))

	c, err := wrappedDriver.Open(name)
	require.Error(t, err)
	require.Zero(t, c)
}

func newMockErrorLogger(ctrl *gomock.Controller) *logmocks.MockLogger {
	logger := logmocks.NewMockLogger(ctrl)
	logger.EXPECT().Errorf(gomock.Any(), gomock.Any()).Times(1)
	return logger
}

// DriverContext
func (s *driverTestSuite) TestDriverContext_OpenConnector_Logf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	name := "dsn"
	mockDriverContext := mocks.NewMockDriver(ctrl)
	mockDriverContext.EXPECT().OpenConnector(name).Return(nil, nil)
	wrappedDriver := newDriver(nil, mockDriverContext, newMockLogLogger(ctrl))

	c, err := wrappedDriver.OpenConnector(name)

	require.NoError(t, err)
	require.NotZero(t, c)
}

func (s *driverTestSuite) TestDriverContext_OpenConnector_Errorf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	name := "dsn"
	mockDriverContext := mocks.NewMockDriver(ctrl)
	mockDriverContext.EXPECT().OpenConnector(name).Return(nil, errors.New("mock OpenConnector Error"))
	wrappedDriver := newDriver(nil, mockDriverContext, newMockErrorLogger(ctrl))

	c, err := wrappedDriver.OpenConnector(name)
	require.Error(t, err)
	require.Zero(t, c)
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

// Connector 测试用例
func (s *driverTestSuite) TestConnector_Connect_Logf() {

}

func (s *driverTestSuite) TestConnector_Connect_Errorf() {

}

func (s *driverTestSuite) TestConnector_Driver_Logf() {

}

func (s *driverTestSuite) TestConnector_Driver_Errorf() {

}

// Stmt 测试用例
func (s *driverTestSuite) TestStmt_Exec_Logf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := logmocks.NewMockLogger(ctrl)
	stmt := mocks.NewMockStmt(ctrl)

	wrappedStmt := &stmtWrapper{stmt: stmt, logger: logger}

	stmt.EXPECT().Exec([]driver.Value{"arg1"}).Return(nil, nil).Times(1)
	logger.EXPECT().Logf("Execute statement with args: %v", []driver.Value{"arg1"}).Times(1)

	_, err := wrappedStmt.Exec([]driver.Value{"arg1"})
	assert.NoError(t, err)
}

func (s *driverTestSuite) TestStmt_Exec_Errorf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	values := []driver.Value{"arg1"}
	expectedError := errors.New("execution failed")
	stmt := mocks.NewMockStmt(ctrl)
	stmt.EXPECT().Exec(values).Return(nil, expectedError).Times(1)

	wrappedStmt := &stmtWrapper{stmt: stmt, logger: newMockErrorLogger(ctrl)}

	_, err := wrappedStmt.Exec(values)
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
}

// Tx 测试用例
func (s *driverTestSuite) TestTx_Commit_Logf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := logmocks.NewMockLogger(ctrl)
	tx := mocks.NewMockTx(ctrl)

	wrappedTx := &txWrapper{tx: tx, logger: logger}

	tx.EXPECT().Commit().Return(nil).Times(1)
	logger.EXPECT().Logf("Commit transaction").Times(1)

	err := wrappedTx.Commit()
	assert.NoError(t, err)
}

func (s *driverTestSuite) TestTx_Commit_Errorf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expectedError := errors.New("commit failed")
	tx := mocks.NewMockTx(ctrl)
	tx.EXPECT().Commit().Return(expectedError).Times(1)

	wrappedTx := &txWrapper{tx: tx, logger: newMockErrorLogger(ctrl)}

	err := wrappedTx.Commit()
	assert.Error(t, err)
	assert.ErrorIs(t, err, expectedError)
}

// Rows 测试用例
func (s *driverTestSuite) TestRows_Close_Logf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := logmocks.NewMockLogger(ctrl)
	rows := mocks.NewMockRows(ctrl)

	wrappedRows := &rowsWrapper{rows: rows, logger: logger}

	rows.EXPECT().Close().Return(nil).Times(1)
	logger.EXPECT().Logf("Close rows").Times(1)

	err := wrappedRows.Close()
	assert.NoError(t, err)
}

// Result 测试用例
func (s *driverTestSuite) TestResult_LastInsertId_Logf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := logmocks.NewMockLogger(ctrl)
	mockRes := mocks.NewMockResult(ctrl)

	wrappedResult := &resultWrapper{result: mockRes, logger: logger}

	mockRes.EXPECT().LastInsertId().Return(int64(1), nil).Times(1)
	logger.EXPECT().Logf("LastInsertId: %d", int64(1)).Times(1)

	id, err := wrappedResult.LastInsertId()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), id)
}

func (s *driverTestSuite) TestResult_LastInsertId_Errorf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := mocks.NewMockResult(ctrl)

	wrappedResult := &resultWrapper{result: mockRes, logger: newMockErrorLogger(ctrl)}

	expectedError := errors.New("error retrieving last insert id")
	mockRes.EXPECT().LastInsertId().Return(int64(0), expectedError).Times(1)

	id, err := wrappedResult.LastInsertId()
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	assert.Equal(t, int64(0), id)
}

func (s *driverTestSuite) TestResult_RowsAffected_Logf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger := logmocks.NewMockLogger(ctrl)
	mockRes := mocks.NewMockResult(ctrl)

	wrappedResult := &resultWrapper{result: mockRes, logger: logger}

	mockRes.EXPECT().RowsAffected().Return(int64(5), nil).Times(1)
	logger.EXPECT().Logf("RowsAffected: %d", int64(5)).Times(1)

	rows, err := wrappedResult.RowsAffected()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), rows)
}

func (s *driverTestSuite) TestResult_RowsAffected_Errorf() {
	t := s.T()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRes := mocks.NewMockResult(ctrl)

	wrappedResult := &resultWrapper{result: mockRes, logger: newMockErrorLogger(ctrl)}

	expectedError := errors.New("error retrieving rows affected")
	mockRes.EXPECT().RowsAffected().Return(int64(0), expectedError).Times(1)

	rows, err := wrappedResult.RowsAffected()
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	assert.Equal(t, int64(0), rows)
}
