package log

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"

	"github.com/meoying/dbproxy/internal/driver/mysql/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestRowsWrapper_Close(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rows := mocks.NewMockRows(ctrl)
		rows.EXPECT().Close().Return(nil).Times(1)

		var rows2 driver.Rows = rows
		var logger logger = newMockInfoLogger(ctrl)
		err := (&rowsWrapper{rows: rows2, logger: logger}).Close()
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rows := mocks.NewMockRows(ctrl)
		expectedError := errors.New("mock close error")
		rows.EXPECT().Close().Return(expectedError).Times(1)

		var rows2 driver.Rows = rows
		var logger logger = newMockErrorLogger(ctrl)
		err := (&rowsWrapper{rows: rows2, logger: logger}).Close()
		assert.Error(t, err)
	})
}

func TestRowsWrapper_Columns(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rows := mocks.NewMockRows(ctrl)
		expectedColumns := []string{"col1", "col2"}
		rows.EXPECT().Columns().Return(expectedColumns).Times(1)

		var rows2 driver.Rows = rows
		var logger logger = newMockInfoLogger(ctrl)
		columns := (&rowsWrapper{rows: rows2, logger: logger}).Columns()
		assert.Equal(t, expectedColumns, columns)
	})
}

func TestRowsWrapper_Next(t *testing.T) {

	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rows := mocks.NewMockRows(ctrl)
		dest := make([]driver.Value, 2)
		rows.EXPECT().Next(dest).Return(nil).Times(1)

		var rows2 driver.Rows = rows
		var logger logger = newMockInfoLogger(ctrl)
		err := (&rowsWrapper{rows: rows2, logger: logger}).Next(dest)
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rows := mocks.NewMockRows(ctrl)
		dest := make([]driver.Value, 2)
		expectedError := errors.New("mock next error")
		rows.EXPECT().Next(dest).Return(expectedError).Times(1)

		var rows2 driver.Rows = rows
		var logger logger = newMockErrorLogger(ctrl)
		err := (&rowsWrapper{rows: rows2, logger: logger}).Next(dest)
		assert.Error(t, err)
	})
}

func TestRowsColumnTypePrecisionScaleWrapper_ColumnTypePrecisionScale(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rows := mocks.NewMockRows(ctrl)
	index := 1
	expectedPrecision, expectedScale := int64(10), int64(2)
	rows.EXPECT().ColumnTypePrecisionScale(index).Return(expectedPrecision, expectedScale, true).Times(1)

	var r driver.RowsColumnTypePrecisionScale = rows
	var logger logger = newMockInfoLogger(ctrl)
	precision, scale, ok := (&rowsWrapper{rows: r, logger: logger}).ColumnTypePrecisionScale(index)
	assert.True(t, ok)
	assert.Equal(t, expectedPrecision, precision)
	assert.Equal(t, expectedScale, scale)
}

func TestRowsColumnTypeNullableWrapper_ColumnTypeNullable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rows := mocks.NewMockRows(ctrl)
	index := 1
	rows.EXPECT().ColumnTypeNullable(index).Return(true, true).Times(1)

	var r driver.RowsColumnTypeNullable = rows
	var logger logger = newMockInfoLogger(ctrl)
	nullable, ok := (&rowsWrapper{rows: r, logger: logger}).ColumnTypeNullable(index)
	assert.True(t, ok)
	assert.True(t, nullable)
}

func TestRowsNextResultSetWrapper_HasNextResultSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rows := mocks.NewMockRows(ctrl)
	rows.EXPECT().HasNextResultSet().Return(true).Times(1)

	var r driver.RowsNextResultSet = rows
	var logger logger = newMockInfoLogger(ctrl)
	hasNext := (&rowsWrapper{rows: r, logger: logger}).HasNextResultSet()
	assert.True(t, hasNext)
}

func TestRowsNextResultSetWrapper_NextResultSet(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rows := mocks.NewMockRows(ctrl)
		rows.EXPECT().NextResultSet().Return(nil).Times(1)

		var r driver.RowsNextResultSet = rows
		var logger logger = newMockInfoLogger(ctrl)
		err := (&rowsWrapper{rows: r, logger: logger}).NextResultSet()
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		rows := mocks.NewMockRows(ctrl)
		expectedError := errors.New("mock next result set error")
		rows.EXPECT().NextResultSet().Return(expectedError).Times(1)

		var r driver.RowsNextResultSet = rows
		var logger logger = newMockErrorLogger(ctrl)
		err := (&rowsWrapper{rows: r, logger: logger}).NextResultSet()
		assert.Error(t, err)
	})
}

func TestRowsColumnTypeScanTypeWrapper_ColumnTypeScanType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rows := mocks.NewMockRows(ctrl)
	index := 1
	expectedType := reflect.TypeOf("")
	rows.EXPECT().ColumnTypeScanType(index).Return(expectedType).Times(1)

	var r driver.RowsColumnTypeScanType = rows
	var logger logger = newMockInfoLogger(ctrl)
	scanType := (&rowsWrapper{rows: r, logger: logger}).ColumnTypeScanType(index)
	assert.Equal(t, expectedType, scanType)
}

func TestRowsColumnTypeDatabaseTypeNameWrapper_ColumnTypeDatabaseTypeName(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	rows := mocks.NewMockRows(ctrl)
	index := 1
	expectedName := "VARCHAR"
	rows.EXPECT().ColumnTypeDatabaseTypeName(index).Return(expectedName).Times(1)

	var r driver.RowsColumnTypeDatabaseTypeName = rows
	var logger logger = newMockInfoLogger(ctrl)
	dbTypeName := (&rowsWrapper{rows: r, logger: logger}).ColumnTypeDatabaseTypeName(index)
	assert.Equal(t, expectedName, dbTypeName)
}
