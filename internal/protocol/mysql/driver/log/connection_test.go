package log

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/driver/mocks"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

func TestConnWrapper_Prepare(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users`"
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().PrepareContext(gomock.Any(), query).Return(&stmtWrapper{}, nil).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockInfoLogger(ctrl)}

		stmt, err := wrappedConn.Prepare(query)
		assert.NoError(t, err)
		assert.NotZero(t, stmt)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users`"
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().PrepareContext(gomock.Any(), query).Return(nil, errors.New("mock PrepareContext error")).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockErrorLogger(ctrl)}

		stmt, err := wrappedConn.Prepare(query)
		assert.Error(t, err)
		assert.Zero(t, stmt)
	})
}

func TestConnWrapper_Close(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().Close().Return(nil).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockInfoLogger(ctrl)}

		err := wrappedConn.Close()
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().Close().Return(errors.New("mock Close error")).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockErrorLogger(ctrl)}

		err := wrappedConn.Close()
		assert.Error(t, err)
	})
}

func TestConnWrapper_Begin(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		opts := driver.TxOptions{}
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().BeginTx(gomock.Any(), opts).Return(&txWrapper{}, nil).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockInfoLogger(ctrl)}

		tx, err := wrappedConn.Begin()
		assert.NoError(t, err)
		assert.NotZero(t, tx)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		opts := driver.TxOptions{}
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().BeginTx(gomock.Any(), opts).Return(nil, errors.New("mock BeginTx error")).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockErrorLogger(ctrl)}

		tx, err := wrappedConn.Begin()
		assert.Error(t, err)
		assert.Zero(t, tx)
	})
}

func TestConnWrapper_PrepareContext(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users`"
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().PrepareContext(gomock.Any(), query).Return(&stmtWrapper{}, nil).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockInfoLogger(ctrl)}

		stmt, err := wrappedConn.PrepareContext(context.Background(), query)
		assert.NoError(t, err)
		assert.NotZero(t, stmt)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users`"
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().PrepareContext(gomock.Any(), query).Return(nil, errors.New("mock PrepareContext error")).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockErrorLogger(ctrl)}

		stmt, err := wrappedConn.PrepareContext(context.Background(), query)
		assert.Error(t, err)
		assert.Zero(t, stmt)
	})
}

func TestConnWrapper_Ping(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pinger := mocks.NewMockConn(ctrl)
		pinger.EXPECT().Ping(gomock.Any()).Return(nil).Times(1)
		wrappedPinger := &connWrapper{conn: pinger, logger: newMockInfoLogger(ctrl)}

		err := wrappedPinger.Ping(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pinger := mocks.NewMockConn(ctrl)
		pinger.EXPECT().Ping(gomock.Any()).Return(errors.New("mock Ping error")).Times(1)
		wrappedPinger := &connWrapper{conn: pinger, logger: newMockErrorLogger(ctrl)}

		err := wrappedPinger.Ping(context.Background())
		assert.Error(t, err)
	})
}

func TestConnWrapper_ExecContext(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "UPDATE `users` SET `name` = ? WHERE `id` = ?"
		args := []driver.NamedValue{{Name: "name", Value: "John"}, {Name: "id", Value: 1}}
		execer := mocks.NewMockConn(ctrl)
		mockResult := mocks.NewMockResult(ctrl)
		execer.EXPECT().ExecContext(gomock.Any(), query, args).Return(mockResult, nil).Times(1)
		wrappedExecer := &connWrapper{conn: execer, logger: newMockInfoLogger(ctrl)}

		result, err := wrappedExecer.ExecContext(context.Background(), query, args)
		assert.NoError(t, err)
		assert.NotZero(t, result)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "UPDATE `users` SET `name` = ? WHERE `id` = ?"
		args := []driver.NamedValue{{Name: "name", Value: "John"}, {Name: "id", Value: 1}}
		execer := mocks.NewMockConn(ctrl)
		execer.EXPECT().ExecContext(gomock.Any(), query, args).Return(nil, errors.New("mock ExecContext error")).Times(1)
		wrappedExecer := &connWrapper{conn: execer, logger: newMockErrorLogger(ctrl)}

		result, err := wrappedExecer.ExecContext(context.Background(), query, args)
		assert.Error(t, err)
		assert.Zero(t, result)
	})
}

func TestConnWrapper_QueryContext(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users` WHERE `id` = ?"
		args := []driver.NamedValue{{Name: "id", Value: 1}}
		queryer := mocks.NewMockConn(ctrl)
		queryer.EXPECT().QueryContext(gomock.Any(), query, args).Return(&rowsWrapper{}, nil).Times(1)
		wrappedQueryer := &connWrapper{conn: queryer, logger: newMockInfoLogger(ctrl)}

		rows, err := wrappedQueryer.QueryContext(context.Background(), query, args)
		assert.NoError(t, err)
		assert.NotZero(t, rows)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users` WHERE `id` = ?"
		args := []driver.NamedValue{{Name: "id", Value: 1}}
		queryer := mocks.NewMockConn(ctrl)
		queryer.EXPECT().QueryContext(gomock.Any(), query, args).Return(nil, errors.New("mock QueryContext error")).Times(1)
		wrappedQueryer := &connWrapper{conn: queryer, logger: newMockErrorLogger(ctrl)}

		rows, err := wrappedQueryer.QueryContext(context.Background(), query, args)
		assert.Error(t, err)
		assert.Zero(t, rows)
	})
}

func TestConnWrapper_BeginTx(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		opts := driver.TxOptions{}
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().BeginTx(gomock.Any(), opts).Return(&txWrapper{}, nil).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockInfoLogger(ctrl)}

		tx, err := wrappedConn.BeginTx(context.Background(), opts)
		assert.NoError(t, err)

		assert.NotZero(t, tx)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		opts := driver.TxOptions{}
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().BeginTx(gomock.Any(), opts).Return(nil, errors.New("mock BeginTx error")).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockErrorLogger(ctrl)}

		tx, err := wrappedConn.BeginTx(context.Background(), opts)
		assert.Error(t, err)
		assert.Zero(t, tx)
	})
}

func TestConnWrapper_ResetSession(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		resetter := mocks.NewMockConn(ctrl)
		resetter.EXPECT().ResetSession(gomock.Any()).Return(nil).Times(1)
		wrappedResetter := &connWrapper{conn: resetter, logger: newMockInfoLogger(ctrl)}

		err := wrappedResetter.ResetSession(context.Background())
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		resetter := mocks.NewMockConn(ctrl)
		resetter.EXPECT().ResetSession(gomock.Any()).Return(errors.New("mock ResetSession error")).Times(1)
		wrappedResetter := &connWrapper{conn: resetter, logger: newMockErrorLogger(ctrl)}

		err := wrappedResetter.ResetSession(context.Background())
		assert.Error(t, err)
	})
}

func TestConnWrapper_IsValid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	validator := mocks.NewMockConn(ctrl)
	validator.EXPECT().IsValid().Return(true).Times(1)
	wrappedValidator := &connWrapper{conn: validator, logger: newMockInfoLogger(ctrl)}

	valid := wrappedValidator.IsValid()
	assert.True(t, valid)
}

func TestConnWrapper_CheckNamedValue(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockChecker := mocks.NewMockConn(ctrl)
		mockChecker.EXPECT().CheckNamedValue(&driver.NamedValue{Name: "arg1"}).Return(nil).Times(1)

		wrappedChecker := &connWrapper{conn: mockChecker, logger: newMockInfoLogger(ctrl)}

		err := wrappedChecker.CheckNamedValue(&driver.NamedValue{Name: "arg1"})
		assert.NoError(t, err)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		value := &driver.NamedValue{Name: "arg1"}
		expectedError := errors.New("mock check named value error")
		mockChecker := mocks.NewMockConn(ctrl)
		mockChecker.EXPECT().CheckNamedValue(value).Return(expectedError).Times(1)

		wrappedChecker := &connWrapper{conn: mockChecker, logger: newMockErrorLogger(ctrl)}

		err := wrappedChecker.CheckNamedValue(value)
		assert.Error(t, err)
		assert.Equal(t, expectedError, err)
	})
}
