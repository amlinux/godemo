// Package frontend implements GSTP WebSocket frontend. This frontend accepts
// incoming connections, checks user cookies, locates users, locates location
// servers, and connects to them.
package frontend

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"git.rulezz.ru/games/metagam/go/auth"
	pb "git.rulezz.ru/games/metagam/go/frontend"
	"git.rulezz.ru/games/metagam/go/layout"
	"git.rulezz.ru/games/metagam/go/sessions"
	"golang.org/x/net/context"
	"golang.org/x/net/websocket"
	"google.golang.org/grpc"
)

const (
	maxMessageSize          = 65536
	sendChanBuf             = 256
	dispatchChanBuf         = 256
	pingInterval            = 30 * time.Second
	locateTimeout           = 20 * time.Second
	sessionGetTimeout       = 20 * time.Second
	authLoginTimeout        = 20 * time.Second
	sendTimeout             = pingInterval
	recvTimeout             = pingInterval * 2
	recvProcessingTimeout   = 10 * time.Second
	locationConnectTimeout  = 10 * time.Second
	locationMetadataTimeout = 5 * time.Second
)

var (
	errBadOrigin = errors.New("origin not allowed")
	errNoCookie  = errors.New("no cookie")
	errBadCookie = errors.New("bad cookie")
)

// Server is a frontend server handling incoming WebSocket connections, and proxying them to the GSTP server.
type Server struct {
	CookieName string
	Chat       bool
	Location   bool

	// Client connections.
	Auth                AuthClient
	Layout              LayoutClient
	Sessions            SessionsClient
	UserLocationService UserLocationServiceClient

	// Map from location type to the location type descriptor.
	LocationTypes map[string]*LocationType

	// How to handle game-specific commands from the client.
	GameCommandHandler func(*Connection, []byte) error

	clients      map[*Connection]struct{}
	clientsMutex sync.RWMutex
}

// LocationType is a location type descriptor.
type LocationType struct {
	Image          string
	Command        string
	ResourceCPU    string
	ResourceMemory string
	Args           []string
}

// AuthClient is an interface defining necessary functions to query the auth server.
type AuthClient interface {
	Login(ctx context.Context, in *auth.LoginRequest, opts ...grpc.CallOption) (*auth.LoginReply, error)
}

// LayoutClient is an interface defining necessary functions to query the layout server.
type LayoutClient interface {
	Get(ctx context.Context, in *layout.GetRequest, opts ...grpc.CallOption) (*layout.GetReply, error)
}

// SessionsClient is an interface defining necessary functions to query the sessions server.
type SessionsClient interface {
	Get(ctx context.Context, in *sessions.GetRequest, opts ...grpc.CallOption) (*sessions.GetReply, error)
}

// UserLocationServiceClient is an interface defining necessary functions to query the user server about user location.
type UserLocationServiceClient interface {
	GetUserLocation(ctx context.Context, in *pb.GetUserLocationRequest, opts ...grpc.CallOption) (*pb.GetUserLocationReply, error)
}

// Handler implements a Websocket connection handler.
func (s *Server) Handler(ws *websocket.Conn) {
	defer ws.Close()
	if err := s.handle(ws); err != nil && err != io.EOF {
		log.Printf("client connection aborted: %v", err)
	}
}

// Handshake implements Websocket handshake procedure.
func (s *Server) Handshake(cfg *websocket.Config, r *http.Request) error {
	return nil
}

// authenticateWithCookie authenticates the request with session cookie. If successful, returns the user ID.
func (s *Server) authenticateWithCookie(r *http.Request) (string, error) {
	origin := r.Header.Get("Origin")
	host := r.Host
	if origin != "http://"+host && origin != "https://"+host {
		return "", errBadOrigin
	}

	// Get the cookie.
	cookie, err := r.Cookie(s.CookieName)
	if err != nil {
		return "", errNoCookie
	}

	// Get session information.
	getCtx, _ := context.WithTimeout(context.Background(), sessionGetTimeout)
	sessReply, err := s.Sessions.Get(getCtx, &sessions.GetRequest{Cookie: cookie.Value})
	if err != nil {
		return "", fmt.Errorf("sessions.Get: %v", err)
	}
	if sessReply.Status != sessions.Status_OK {
		if sessReply.Status == sessions.Status_NOT_FOUND {
			return "", errBadCookie
		}
		return "", fmt.Errorf("error getting session: %s", sessReply.Status)
	}
	user := sessReply.Session.User
	log.Printf("User %s authenticated with cookie", user)
	return user, nil

}

func (s *Server) handle(ws *websocket.Conn) error {
	ws.PayloadType = websocket.BinaryFrame
	if err := ws.SetDeadline(time.Time{}); err != nil {
		return fmt.Errorf("error setting deadline: %v", err)
	}
	r := ws.Request()

	// Validate connection.
	if r.Method != "GET" {
		return fmt.Errorf("got method %s, want GET", r.Method)
	}

	// Start new context.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &Connection{
		s:          s,
		ws:         ws,
		ctx:        ctx,
		errCh:      make(chan error, 1),
		sendCh:     make(chan *pb.ServerMessage, sendChanBuf),
		dispatchCh: make(chan *pb.ClientMessage, dispatchChanBuf),
		locCh:      make(chan *Location),
	}

	user, err := s.authenticateWithCookie(r)
	if err != nil {
		if err != errNoCookie && err != errBadOrigin && err != errBadCookie {
			return err
		}
		c.state = stateUnauthenticated
	} else {
		c.state = stateAuthenticated
		c.user = user
	}

	go c.sendLoop()
	go c.recvLoop()
	go c.dispatchLoop()

	s.registerClient(c)
	defer s.unregisterClient(c)

	return <-c.errCh
}

func (s *Server) registerClient(c *Connection) {
	s.clientsMutex.Lock()
	defer s.clientsMutex.Unlock()
	if s.clients == nil {
		s.clients = make(map[*Connection]struct{})
	}
	s.clients[c] = struct{}{}
}

func (s *Server) unregisterClient(c *Connection) {
	s.clientsMutex.Lock()
	defer s.clientsMutex.Unlock()
	if s.clients == nil {
		s.clients = make(map[*Connection]struct{})
	}
	delete(s.clients, c)
}

// Broadcast sends a message to all connected clients.
func (s *Server) Broadcast(m *pb.ServerMessage) {
	s.clientsMutex.RLock()
	defer s.clientsMutex.RUnlock()
	for c := range s.clients {
		c.send(m)
	}
}
