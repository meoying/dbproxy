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

func TestStmtWrapper_Exec(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStmt := mocks.NewMockStmt(ctrl)
		mockResult := mocks.NewMockResult(ctrl)
		mockStmt.EXPECT().Exec([]driver.Value{"arg1"}).Return(mockResult, nil).Times(1)

		wrappedStmt := &stmtWrapper{stmt: mockStmt, logger: newMockLogLogger(ctrl)}

		result, err := wrappedStmt.Exec([]driver.Value{"arg1"})
		assert.NoError(t, err)
		assert.NotZero(t, result)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		values := []driver.Value{"arg1"}
		expectedError := errors.New("mock exec error")
		stmt := mocks.NewMockStmt(ctrl)
		stmt.EXPECT().Exec(values).Return(nil, expectedError).Times(1)

		wrappedStmt := &stmtWrapper{stmt: stmt, logger: newMockErrorLogger(ctrl)}

		_, err := wrappedStmt.Exec(values)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
	})
}

func TestStmtWrapper_Query(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStmt := mocks.NewMockStmt(ctrl)
		mockRows := mocks.NewMockRows(ctrl)
		mockStmt.EXPECT().Query([]driver.Value{"arg1"}).Return(mockRows, nil).Times(1)

		wrappedStmt := &stmtWrapper{stmt: mockStmt, logger: newMockLogLogger(ctrl)}

		rows, err := wrappedStmt.Query([]driver.Value{"arg1"})
		assert.NoError(t, err)
		assert.NotZero(t, rows)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		values := []driver.Value{"arg1"}
		expectedError := errors.New("mock query error")
		stmt := mocks.NewMockStmt(ctrl)
		stmt.EXPECT().Query(values).Return(nil, expectedError).Times(1)

		wrappedStmt := &stmtWrapper{stmt: stmt, logger: newMockErrorLogger(ctrl)}

		_, err := wrappedStmt.Query(values)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
	})
}

func TestStmtWrapper_NumInput(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStmt := mocks.NewMockStmt(ctrl)
	mockStmt.EXPECT().NumInput().Return(1).Times(1)

	wrappedStmt := &stmtWrapper{stmt: mockStmt, logger: newMockLogLogger(ctrl)}

	numInput := wrappedStmt.NumInput()
	assert.Equal(t, 1, numInput)
}

func TestStmtWrapper_Close(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStmt := mocks.NewMockStmt(ctrl)
		mockStmt.EXPECT().Close().Return(nil).Times(1)

		wrappedStmt := &stmtWrapper{stmt: mockStmt, logger: newMockLogLogger(ctrl)}

		err := wrappedStmt.Close()
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedError := errors.New("mock close error")
		stmt := mocks.NewMockStmt(ctrl)
		stmt.EXPECT().Close().Return(expectedError).Times(1)

		wrappedStmt := &stmtWrapper{stmt: stmt, logger: newMockErrorLogger(ctrl)}

		err := wrappedStmt.Close()
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
	})
}

func TestStmtQueryContextWrapper_QueryContext(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStmt := mocks.NewMockStmt(ctrl)
		mockRows := mocks.NewMockRows(ctrl)
		mockStmt.EXPECT().QueryContext(gomock.Any(), []driver.NamedValue{{Name: "arg1"}}).Return(mockRows, nil).Times(1)

		wrappedStmt := &stmtWrapper{stmt: mockStmt, logger: newMockLogLogger(ctrl)}

		rows, err := wrappedStmt.QueryContext(context.Background(), []driver.NamedValue{{Name: "arg1"}})
		assert.NoError(t, err)
		assert.NotZero(t, rows)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		values := []driver.NamedValue{{Name: "arg1"}}
		expectedError := errors.New("mock query context error")
		stmt := mocks.NewMockStmt(ctrl)
		stmt.EXPECT().QueryContext(gomock.Any(), values).Return(nil, expectedError).Times(1)

		wrappedStmt := &stmtWrapper{stmt: stmt, logger: newMockErrorLogger(ctrl)}

		_, err := wrappedStmt.QueryContext(context.Background(), values)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
	})
}

func TestStmtExecContextWrapper_ExecContext(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockStmt := mocks.NewMockStmt(ctrl)
		mockResult := mocks.NewMockResult(ctrl)
		mockStmt.EXPECT().ExecContext(gomock.Any(), []driver.NamedValue{{Name: "arg1"}}).Return(mockResult, nil).Times(1)

		wrappedStmt := &stmtWrapper{stmt: mockStmt, logger: newMockLogLogger(ctrl)}

		result, err := wrappedStmt.ExecContext(context.Background(), []driver.NamedValue{{Name: "arg1"}})
		assert.NoError(t, err)
		assert.NotZero(t, result)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		values := []driver.NamedValue{{Name: "arg1"}}
		expectedError := errors.New("mock exec context error")
		stmt := mocks.NewMockStmt(ctrl)
		stmt.EXPECT().ExecContext(gomock.Any(), values).Return(nil, expectedError).Times(1)

		wrappedStmt := &stmtWrapper{stmt: stmt, logger: newMockErrorLogger(ctrl)}

		_, err := wrappedStmt.ExecContext(context.Background(), values)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
	})
}

func TestNamedValueCheckerWrapper_CheckNamedValue(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockChecker := mocks.NewMockStmt(ctrl)
		mockChecker.EXPECT().CheckNamedValue(&driver.NamedValue{Name: "arg1"}).Return(nil).Times(1)

		wrappedChecker := &stmtWrapper{stmt: mockChecker, logger: newMockLogLogger(ctrl)}

		err := wrappedChecker.CheckNamedValue(&driver.NamedValue{Name: "arg1"})
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		value := &driver.NamedValue{Name: "arg1"}
		expectedError := errors.New("mock check named value error")
		checker := mocks.NewMockStmt(ctrl)
		checker.EXPECT().CheckNamedValue(value).Return(expectedError).Times(1)

		wrappedChecker := &stmtWrapper{stmt: checker, logger: newMockErrorLogger(ctrl)}

		err := wrappedChecker.CheckNamedValue(value)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
	})
}

func TestColumnConverterWrapper_ColumnConverter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockConverter := mocks.NewMockStmt(ctrl)
	mockConverter.EXPECT().ColumnConverter(1).Return(driver.DefaultParameterConverter).Times(1)

	wrappedConverter := &stmtWrapper{stmt: mockConverter, logger: newMockLogLogger(ctrl)}

	converter := wrappedConverter.ColumnConverter(1)
	assert.Equal(t, driver.DefaultParameterConverter, converter)
}
