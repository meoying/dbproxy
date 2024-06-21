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

func TestDriver(t *testing.T) {

	t.Run("Open", func(t *testing.T) {

		t.Run("Logf", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			name := "dsn"
			mockDriver := mocks.NewMockDriver(ctrl)
			mockDriver.EXPECT().Open(name).Return(nil, nil)
			wrappedDriver := newDriver(mockDriver, nil, newMockLogLogger(ctrl))

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
			wrappedDriver := newDriver(mockDriver, nil, newMockErrorLogger(ctrl))

			c, err := wrappedDriver.Open(name)
			require.Error(t, err)
			require.Zero(t, c)
		})

	})

	t.Run("OpenConnector", func(t *testing.T) {
		t.Run("Logf", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			name := "dsn"
			mockDriverContext := mocks.NewMockDriver(ctrl)
			mockDriverContext.EXPECT().OpenConnector(name).Return(nil, nil)
			wrappedDriver := newDriver(nil, mockDriverContext, newMockLogLogger(ctrl))

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
			wrappedDriver := newDriver(nil, mockDriverContext, newMockErrorLogger(ctrl))

			c, err := wrappedDriver.OpenConnector(name)
			require.Error(t, err)
			require.Zero(t, c)
		})
	})
}

func newMockLogLogger(ctrl *gomock.Controller) *logmocks.MockLogger {
	logger := logmocks.NewMockLogger(ctrl)
	logger.EXPECT().Logf(gomock.Any(), gomock.Any()).Times(1)
	return logger
}

func newMockErrorLogger(ctrl *gomock.Controller) *logmocks.MockLogger {
	logger := logmocks.NewMockLogger(ctrl)
	logger.EXPECT().Errorf(gomock.Any(), gomock.Any()).Times(1)
	return logger
}

func TestLogDriverTestSuite(t *testing.T) {
	suite.Run(t, new(driverTestSuite))
}

type driverTestSuite struct {
	suite.Suite
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
