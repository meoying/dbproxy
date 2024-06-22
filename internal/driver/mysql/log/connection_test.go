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

func TestConnWrapper_Prepare(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users`"
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().Prepare(query).Return(&stmtWrapper{}, nil).Times(1)

		wrappedConn := &connWrapper{conn: conn, logger: newMockLogLogger(ctrl)}

		stmt, err := wrappedConn.Prepare(query)
		assert.NoError(t, err)
		assert.NotZero(t, stmt)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users`"
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().Prepare(query).Return(nil, errors.New("mock Prepare error")).Times(1)
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
		wrappedConn := &connWrapper{conn: conn, logger: newMockLogLogger(ctrl)}

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

		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().Begin().Return(&txWrapper{}, nil).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockLogLogger(ctrl)}

		tx, err := wrappedConn.Begin()
		assert.NoError(t, err)
		assert.NotZero(t, tx)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().Begin().Return(nil, errors.New("mock Begin error")).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockErrorLogger(ctrl)}

		tx, err := wrappedConn.Begin()
		assert.Error(t, err)
		assert.Zero(t, tx)
	})
}

func TestConnPrepareContextWrapper_PrepareContext(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users`"
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().PrepareContext(gomock.Any(), query).Return(&stmtWrapper{}, nil).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockLogLogger(ctrl)}

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

func TestPingerWrapper_Ping(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		pinger := mocks.NewMockConn(ctrl)
		pinger.EXPECT().Ping(gomock.Any()).Return(nil).Times(1)
		wrappedPinger := &connWrapper{conn: pinger, logger: newMockLogLogger(ctrl)}

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

func TestExecContextWrapper_ExecContext(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "UPDATE `users` SET `name` = ? WHERE `id` = ?"
		args := []driver.NamedValue{{Name: "name", Value: "John"}, {Name: "id", Value: 1}}
		execer := mocks.NewMockConn(ctrl)
		mockResult := mocks.NewMockResult(ctrl)
		execer.EXPECT().ExecContext(gomock.Any(), query, args).Return(mockResult, nil).Times(1)
		wrappedExecer := &connWrapper{conn: execer, logger: newMockLogLogger(ctrl)}

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

func TestQueryerContextWrapper_QueryContext(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users` WHERE `id` = ?"
		args := []driver.NamedValue{{Name: "id", Value: 1}}
		queryer := mocks.NewMockConn(ctrl)
		queryer.EXPECT().QueryContext(gomock.Any(), query, args).Return(&rowsWrapper{}, nil).Times(1)
		wrappedQueryer := &connWrapper{conn: queryer, logger: newMockLogLogger(ctrl)}

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

func TestConnBeginTxWrapper_BeginTx(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		opts := driver.TxOptions{}
		conn := mocks.NewMockConn(ctrl)
		conn.EXPECT().BeginTx(gomock.Any(), opts).Return(&txWrapper{}, nil).Times(1)
		wrappedConn := &connWrapper{conn: conn, logger: newMockLogLogger(ctrl)}

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

func TestResetSessionWrapper_ResetSession(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		resetter := mocks.NewMockConn(ctrl)
		resetter.EXPECT().ResetSession(gomock.Any()).Return(nil).Times(1)
		wrappedResetter := &connWrapper{conn: resetter, logger: newMockLogLogger(ctrl)}

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

func TestValidatorWrapper_IsValid(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	validator := mocks.NewMockConn(ctrl)
	validator.EXPECT().IsValid().Return(true).Times(1)
	wrappedValidator := &connWrapper{conn: validator, logger: newMockLogLogger(ctrl)}

	valid := wrappedValidator.IsValid()
	assert.True(t, valid)
}

func TestExecerWrapper_Exec(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "INSERT INTO `users` (`name`) VALUES (?)"
		args := []driver.Value{"John"}
		execer := mocks.NewMockConn(ctrl)
		mockResult := mocks.NewMockResult(ctrl)
		execer.EXPECT().Exec(query, args).Return(mockResult, nil).Times(1)
		wrappedExecer := &connWrapper{conn: execer, logger: newMockLogLogger(ctrl)}

		result, err := wrappedExecer.Exec(query, args)
		assert.NoError(t, err)
		assert.NotZero(t, result)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "INSERT INTO `users` (`name`) VALUES (?)"
		args := []driver.Value{"John"}
		execer := mocks.NewMockConn(ctrl)
		execer.EXPECT().Exec(query, args).Return(nil, errors.New("mock Exec error")).Times(1)
		wrappedExecer := &connWrapper{conn: execer, logger: newMockErrorLogger(ctrl)}

		result, err := wrappedExecer.Exec(query, args)
		assert.Error(t, err)
		assert.Zero(t, result)
	})
}

func TestQueryerWrapper_Query(t *testing.T) {
	t.Run("Logf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users` WHERE `id` = ?"
		args := []driver.Value{1}
		queryer := mocks.NewMockConn(ctrl)
		queryer.EXPECT().Query(query, args).Return(&rowsWrapper{}, nil).Times(1)
		wrappedQueryer := &connWrapper{conn: queryer, logger: newMockLogLogger(ctrl)}

		rows, err := wrappedQueryer.Query(query, args)
		assert.NoError(t, err)
		assert.NotZero(t, rows)
	})

	t.Run("Errorf", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		query := "SELECT * FROM `users` WHERE `id` = ?"
		args := []driver.Value{1}
		queryer := mocks.NewMockConn(ctrl)
		queryer.EXPECT().Query(query, args).Return(nil, errors.New("mock Query error")).Times(1)
		wrappedQueryer := &connWrapper{conn: queryer, logger: newMockErrorLogger(ctrl)}

		rows, err := wrappedQueryer.Query(query, args)
		assert.Error(t, err)
		assert.Zero(t, rows)
	})
}
