package testsuite

import (
	"database/sql"

	"github.com/stretchr/testify/suite"
)

type PrepareStatementTestSuite struct {
	suite.Suite
	db *sql.DB
}

func (s *PrepareStatementTestSuite) SetDB(db *sql.DB) {
	s.db = db
}

func (s *PrepareStatementTestSuite) TestSelect() {
	t := s.T()
	t.Skip()
	// s.db.PrepareContext(context.Background(), "select * from users")
	//
	// tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	// if err != nil {
	// 	log.Fatal(err)
	// }
	//
	// // 插入用户数据
	// insertStmt, err := tx.PrepareContext(context.Background(), "INSERT INTO users (name, email) VALUES (?, ?)")
	// if err != nil {
	// 	tx.Rollback()
	// 	log.Fatal(err)
	// }

}
