package server

import (
	"github.com/src-d/go-mysql-server"

	"github.com/src-d/go-vitess/mysql"
)

type Server struct {
	Listener *mysql.Listener
}

func NewServer(protocol, address string, auth mysql.AuthServer, e *sqle.Engine) (*Server, error) {
	l, err := mysql.NewListener(protocol, address, auth, NewHandler(e))
	if err != nil {
		return nil, err
	}

	return &Server{Listener: l}, nil
}

func (s *Server) Start() error {
	s.Listener.Accept()
	return nil
}
