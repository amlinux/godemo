package frontend

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"testing"

	"git.rulezz.ru/games/metagam/go/auth"
	pb "git.rulezz.ru/games/metagam/go/frontend"
	"git.rulezz.ru/games/metagam/go/sessions"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"golang.org/x/net/websocket"
	"google.golang.org/grpc"
)

type testServer struct {
	ln      net.Listener
	fs      *Server
	ws      *http.Server
	wss     *websocket.Server
	addr    string
	stopped chan interface{}
}

type fakeAuth struct{}

func (a *fakeAuth) Login(ctx context.Context, in *auth.LoginRequest, opts ...grpc.CallOption) (*auth.LoginReply, error) {
	if in.Name != "foo" || in.Password != "bar" {
		return &auth.LoginReply{Status: auth.Status_ACCESS_DENIED}, nil
	}
	return &auth.LoginReply{Id: "12345"}, nil
}

type fakeSessions struct{}

func (s *fakeSessions) Get(ctx context.Context, in *sessions.GetRequest, opts ...grpc.CallOption) (*sessions.GetReply, error) {
	if in.Cookie != "correct_cookie" {
		return &sessions.GetReply{Status: sessions.Status_NOT_FOUND}, nil
	}
	return &sessions.GetReply{Session: &sessions.Session{Id: "correct_cookie", User: "54321", Server: "server0"}}, nil
}

func newTestServer() *testServer {
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		log.Panic(err)
	}
	fs := &Server{
		CookieName: "sess",
		Auth:       &fakeAuth{},
		Sessions:   &fakeSessions{},
	}
	s := &testServer{
		ln:      ln,
		fs:      fs,
		wss:     &websocket.Server{Handshake: fs.Handshake, Handler: fs.Handler},
		addr:    ln.Addr().String(),
		stopped: make(chan interface{}),
	}
	s.ws = &http.Server{Handler: s.wss}
	go func() {
		s.ws.Serve(s.ln)
		close(s.stopped)
	}()
	return s
}

func (s *testServer) stop() {
	s.ln.Close()
	_, _ = <-s.stopped
}

func read(ws *websocket.Conn) (*pb.ServerMessage, error) {
	readBuf := make([]byte, maxMessageSize)
	n, err := ws.Read(readBuf)
	if err != nil {
		return nil, err
	}
	m := new(pb.ServerMessage)
	if err := proto.Unmarshal(readBuf[0:n], m); err != nil {
		return nil, err
	}
	return m, nil
}

func write(ws *websocket.Conn, m *pb.ClientMessage) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	_, err = ws.Write(data)
	return err
}

func TestRequestWithoutCookie(t *testing.T) {
	s := newTestServer()
	defer s.stop()

	cfg, err := websocket.NewConfig(fmt.Sprintf("ws://%s", s.addr), fmt.Sprintf("http://%s", s.addr))
	if err != nil {
		t.Fatal(err)
	}
	ws, err := websocket.DialConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	want := &pb.ServerMessage{Type: pb.ServerMessage_AUTHENTICATE}
	if got, err := read(ws); err != nil || !proto.Equal(got, want) {
		t.Fatalf("first packet from unauthenticated request was (%+v, %v), want (%+v, nil)", got, err, want)
	}
	if err = write(ws, &pb.ClientMessage{Type: pb.ClientMessage_LOGIN, Login: &pb.Login{Name: "foo", Password: "bad_password"}}); err != nil {
		t.Fatalf("error sending login request: %v", err)
	}
	if got, err := read(ws); err != nil || !proto.Equal(got, want) {
		t.Fatalf("bad authentication resulted in response (%+v, %v), want (%+v, nil)", got, err, want)
	}
	if err = write(ws, &pb.ClientMessage{Type: pb.ClientMessage_LOGIN, Login: &pb.Login{Name: "foo", Password: "bar"}}); err != nil {
		t.Fatalf("error sending login request: %v", err)
	}
	want = &pb.ServerMessage{Type: pb.ServerMessage_WELCOME, Welcome: &pb.Welcome{User: "12345"}}
	if got, err := read(ws); err != nil || !proto.Equal(got, want) {
		t.Fatalf("successful authentication resulted in response (%+v, %v), want (%+v, nil)", got, err, want)
	}
}

func TestRequestWithInvalidCookie(t *testing.T) {
	s := newTestServer()
	defer s.stop()

	cfg, err := websocket.NewConfig(fmt.Sprintf("ws://%s", s.addr), fmt.Sprintf("http://%s", s.addr))
	cfg.Header.Add("Cookie", "sess=incorrect_cookie")
	if err != nil {
		t.Fatal(err)
	}
	ws, err := websocket.DialConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	want := &pb.ServerMessage{Type: pb.ServerMessage_AUTHENTICATE}
	if got, err := read(ws); err != nil || !proto.Equal(got, want) {
		t.Fatalf("first packet from unauthenticated request was (%+v, %v), want (%+v, nil)", got, err, want)
	}
}

func TestRequestWithInvalidOrigin(t *testing.T) {
	s := newTestServer()
	defer s.stop()

	cfg, err := websocket.NewConfig(fmt.Sprintf("ws://%s", s.addr), "http://www.bad.com")
	cfg.Header.Add("Cookie", "sess=correct_cookie")
	if err != nil {
		t.Fatal(err)
	}
	ws, err := websocket.DialConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	want := &pb.ServerMessage{Type: pb.ServerMessage_AUTHENTICATE}
	if got, err := read(ws); err != nil || !proto.Equal(got, want) {
		t.Fatalf("first packet from unauthenticated request was (%+v, %v), want (%+v, nil)", got, err, want)
	}
}

func TestRequestWithCookie(t *testing.T) {
	s := newTestServer()
	defer s.stop()

	cfg, err := websocket.NewConfig(fmt.Sprintf("ws://%s", s.addr), fmt.Sprintf("http://%s", s.addr))
	cfg.Header.Add("Cookie", "sess=correct_cookie")
	if err != nil {
		t.Fatal(err)
	}
	ws, err := websocket.DialConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()

	want := &pb.ServerMessage{Type: pb.ServerMessage_WELCOME, Welcome: &pb.Welcome{User: "54321"}}
	if got, err := read(ws); err != nil || !proto.Equal(got, want) {
		t.Fatalf("first packet from request with cookie was (%+v, %v), want (%+v, nil)", got, err, want)
	}
}
