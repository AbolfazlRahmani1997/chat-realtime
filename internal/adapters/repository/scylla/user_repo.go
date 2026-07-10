package scylla

import (
	"strconv"

	"github.com/gocql/gocql"
	"server/internal/core/domain"
)

type userRepo struct {
	session *gocql.Session
}

func NewUserRepo(session *gocql.Session) *userRepo {
	return &userRepo{session: session}
}

func (r *userRepo) GetByID(userID string) (*domain.User, error) {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return nil, err
	}

	var user domain.User
	err = r.session.Query(`SELECT user_id, firstname, lastname, username, avatar_url, last_seen FROM users WHERE user_id = ?`,
		uid).Scan(&uid, &user.FirstName, &user.LastName, &user.UserName, &user.AvatarUrl, &user.LastSeen)
	if err != nil {
		return nil, err
	}
	user.UserId = userID
	return &user, nil
}

func (r *userRepo) Upsert(user *domain.User) error {
	uid, err := strconv.Atoi(user.UserId)
	if err != nil {
		return err
	}
	return r.session.Query(`UPDATE users SET firstname = ?, lastname = ?, username = ?, avatar_url = ? WHERE user_id = ?`,
		user.FirstName, user.LastName, user.UserName, user.AvatarUrl, uid,
	).Exec()
}

func (r *userRepo) UpdateLastSeen(userID string, lastSeen int64) error {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return err
	}
	return r.session.Query(`UPDATE users SET last_seen = ? WHERE user_id = ?`, lastSeen, uid).Exec()
}

func (r *userRepo) GetLastSeen(userID string) (int64, error) {
	uid, err := strconv.Atoi(userID)
	if err != nil {
		return 0, err
	}

	var lastSeen int64
	err = r.session.Query(`SELECT last_seen FROM users WHERE user_id = ?`, uid).Scan(&lastSeen)
	if err != nil {
		return 0, err
	}
	return lastSeen, nil
}
