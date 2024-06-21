package log

import (
	"errors"
	"testing"

	"github.com/meoying/dbproxy/internal/driver/mysql/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestResultWrapper_LastInsertId(t *testing.T) {

	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRes := mocks.NewMockResult(ctrl)
		mockRes.EXPECT().LastInsertId().Return(int64(1), nil).Times(1)

		id, err := newResultWrapper(mockRes, newMockLogLogger(ctrl)).LastInsertId()
		assert.NoError(t, err)
		assert.Equal(t, int64(1), id)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRes := mocks.NewMockResult(ctrl)
		expectedError := errors.New("mock LastInsertId error")
		mockRes.EXPECT().LastInsertId().Return(int64(0), expectedError).Times(1)

		id, err := newResultWrapper(mockRes, newMockErrorLogger(ctrl)).LastInsertId()
		assert.Error(t, err)
		assert.Equal(t, int64(0), id)
	})
}

func TestResultWrapper_RowsAffected(t *testing.T) {

	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRes := mocks.NewMockResult(ctrl)
		mockRes.EXPECT().RowsAffected().Return(int64(10), nil).Times(1)

		rows, err := newResultWrapper(mockRes, newMockLogLogger(ctrl)).RowsAffected()
		assert.NoError(t, err)
		assert.Equal(t, int64(10), rows)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRes := mocks.NewMockResult(ctrl)
		expectedError := errors.New("mock RowsAffected error")
		mockRes.EXPECT().RowsAffected().Return(int64(0), expectedError).Times(1)

		rows, err := newResultWrapper(mockRes, newMockErrorLogger(ctrl)).RowsAffected()
		assert.Error(t, err)
		assert.Equal(t, int64(0), rows)
	})
}
