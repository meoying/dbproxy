package log

import (
	"errors"
	"testing"

	"github.com/meoying/dbproxy/internal/driver/mysql/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestTxWrapper_Commit(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tx := mocks.NewMockTx(ctrl)
		tx.EXPECT().Commit().Return(nil).Times(1)

		wrappedTx := &txWrapper{tx: tx, logger: newMockLogLogger(ctrl)}

		err := wrappedTx.Commit()
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedError := errors.New("commit failed")
		tx := mocks.NewMockTx(ctrl)
		tx.EXPECT().Commit().Return(expectedError).Times(1)

		wrappedTx := &txWrapper{tx: tx, logger: newMockErrorLogger(ctrl)}

		err := wrappedTx.Commit()
		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedError)
	})
}

func TestTxWrapper_Rollback(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tx := mocks.NewMockTx(ctrl)
		tx.EXPECT().Rollback().Return(nil).Times(1)

		wrappedTx := &txWrapper{tx: tx, logger: newMockLogLogger(ctrl)}

		err := wrappedTx.Rollback()
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedError := errors.New("rollback failed")
		tx := mocks.NewMockTx(ctrl)
		tx.EXPECT().Rollback().Return(expectedError).Times(1)

		wrappedTx := &txWrapper{tx: tx, logger: newMockErrorLogger(ctrl)}

		err := wrappedTx.Rollback()
		assert.Error(t, err)
		assert.ErrorIs(t, err, expectedError)
	})
}
