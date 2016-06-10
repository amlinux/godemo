package frontend

import (
	"errors"
	"fmt"
	"log"
	"time"

	"git.rulezz.ru/games/metagam/go/auth"
	pb "git.rulezz.ru/games/metagam/go/frontend"
	"git.rulezz.ru/games/metagam/go/gstp"
	"git.rulezz.ru/games/metagam/go/layout"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"golang.org/x/net/websocket"
	"google.golang.org/grpc"
)

type ConnectionState int

const (
	stateUnauthenticated ConnectionState = iota
	stateAuthenticated   ConnectionState = iota
)

// Location describes location a player is currently in.
type Location struct {
	Type   string
	ID     string
	Client gstp.GSTPClient
}

// Connection represents single client -> frontend connection.
type Connection struct {
	s          *Server
	ws         *websocket.Conn
	ctx        context.Context
	errCh      chan error
	sendCh     chan *pb.ServerMessage
	dispatchCh chan *pb.ClientMessage
	locCh      chan *Location

	// Authentication
	state         ConnectionState
	user          string
	loginAttempts int
	loc           *Location
}

func (c *Connection) send(m *pb.ServerMessage) {
	select {
	case c.sendCh <- m:
	case <-time.After(sendTimeout):
		c.abort(errors.New("sender is not accepting messages"))
	}
}

func (c *Connection) realSend(m *pb.ServerMessage) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	_, err = c.ws.Write(data)
	return err
}

func (c *Connection) abort(err error) {
	select {
	case c.errCh <- err:
	default:
	}
}

func (c *Connection) sendLoop() {
	for {
		select {
		case m := <-c.sendCh:
			if err := c.realSend(m); err != nil {
				c.abort(err)
			}
		case <-c.ctx.Done():
			return
		case <-time.After(pingInterval):
			if err := c.realSend(&pb.ServerMessage{Type: pb.ServerMessage_PING}); err != nil {
				c.abort(err)
			}
		}
	}
}

func (c *Connection) recvLoop() {
	readBuf := make([]byte, maxMessageSize)
	for {
		n, err := c.ws.Read(readBuf)
		select {
		case <-c.ctx.Done():
			// Error might be just because the context was cancelled. In that case just return with error.
			return
		default:
		}
		if err != nil {
			c.abort(err)
			return
		}
		var msg pb.ClientMessage
		if err := proto.Unmarshal(readBuf[0:n], &msg); err != nil {
			log.Printf("received unparsable message with len=%d", n)
			continue
		}
		select {
		case c.dispatchCh <- &msg:
		case <-time.After(recvProcessingTimeout):
			c.abort(errors.New("receiver processor is not accepting messages"))
			return
		}
	}
}

func (c *Connection) dispatchLoop() {
	if c.state == stateUnauthenticated {
		c.send(&pb.ServerMessage{Type: pb.ServerMessage_AUTHENTICATE})
	}
	authenticated := false
	for {
		if !authenticated && c.state != stateUnauthenticated {
			authenticated = true
			c.send(&pb.ServerMessage{Type: pb.ServerMessage_WELCOME, Welcome: &pb.Welcome{User: c.user}})
			if c.s.Location {
				go c.connectToLocation()
			}
		}
		select {
		case m := <-c.dispatchCh:
			if m.Type == pb.ClientMessage_PING {
				break
			}
			switch c.state {
			case stateUnauthenticated:
				if err := c.handleUnauthenticatedMessage(m); err != nil {
					log.Printf("Error handling incoming unauthenticated message %s: %v", proto.CompactTextString(m), err)
				}
			case stateAuthenticated:
				if err := c.handleAuthenticatedMessage(m); err != nil {
					log.Printf("Error handling incoming authenticated message %s: %v", proto.CompactTextString(m), err)
				}
			default:
				log.Printf("Message received in an unknown state %d: %s", c.state, proto.CompactTextString(m))
			}
		case l := <-c.locCh:
			c.loc = l
		case <-c.ctx.Done():
			return
		case <-time.After(recvTimeout):
			c.abort(errors.New("timeout waiting for a message (no even pings)"))
			return
		}
	}
}

func (c *Connection) connectToLocation() {
	for {
		// Check for cancellation.
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		// The whole process is time limited.
		locateCtx, _ := context.WithTimeout(c.ctx, locateTimeout)

		// Get player location.
		locReply, err := c.s.UserLocationService.GetUserLocation(locateCtx, &pb.GetUserLocationRequest{User: c.user})
		if err != nil {
			c.abort(fmt.Errorf("getting user location failed: %v", err))
			return
		}
		log.Printf("User %s: location (%s, %s)", c.user, locReply.LocationType, locReply.LocationId)

		// Get location server IP.
		layoutReply, err := c.s.Layout.Get(locateCtx, &layout.GetRequest{&layout.Location{Type: locReply.LocationType, Id: locReply.LocationId}})
		if err != nil {
			c.abort(fmt.Errorf("locating location server (%s, %s): %v", locReply.LocationType, locReply.LocationId, err))
			return
		}
		if layoutReply.Server == nil {
			c.abort(fmt.Errorf("location server for (%s, %s) not found", locReply.LocationType, locReply.LocationId))
			return
		}
		endpoint := layoutReply.Server.Host + ":5000"
		log.Printf("User %s: connecting to location server (%s, %s) at %s", c.user, locReply.LocationType, locReply.LocationId, endpoint)

		// Connect to the location server.
		locConn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(locationConnectTimeout))
		if err != nil {
			log.Printf("User %s: error connecting to the location server %s: %v", c.user, endpoint, err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Printf("User %s: successfully connected to location server %s", c.user, endpoint)
		locClient := gstp.NewGSTPClient(locConn)
		select {
		case c.locCh <- &Location{Type: locReply.LocationType, ID: locReply.LocationId, Client: locClient}:
		case <-time.After(locationMetadataTimeout):
			log.Printf("User %s: could not deliver location metadata to the dispatcher", c.user)
			locConn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		stream, err := locClient.GetState(c.ctx, &gstp.GetStateRequest{User: c.user, Object: fmt.Sprintf("%s-%s", locReply.LocationType, locReply.LocationId)})
		if err != nil {
			log.Printf("User %s: error getting state of location (%s, %s) from server %s: %v", c.user, locReply.LocationType, locReply.LocationId, endpoint, err)
			locConn.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		c.send(&pb.ServerMessage{Type: pb.ServerMessage_LOCATION_HELLO, Location: &pb.Location{Type: locReply.LocationType, Id: locReply.LocationId}})
		for {
			msg, err := stream.Recv()
			if err != nil {
				log.Printf("User %s: error receiving data of location (%s, %s) from server %s: %v", c.user, locReply.LocationType, locReply.LocationId, endpoint, err)
				break
			}
			c.send(&pb.ServerMessage{Type: pb.ServerMessage_LOCATION_STATE, LocationState: msg})
		}
		locConn.Close()
	}
}

func (c *Connection) handleUnauthenticatedMessage(m *pb.ClientMessage) error {
	switch m.Type {
	case pb.ClientMessage_LOGIN:
		if m.Login == nil {
			c.abort(errors.New("login packet received without login attribute"))
			return nil
		}
		ctx, _ := context.WithTimeout(c.ctx, authLoginTimeout)
		loginReply, err := c.s.Auth.Login(ctx, &auth.LoginRequest{Name: m.Login.Name, Password: m.Login.Password})
		if err != nil {
			c.abort(fmt.Errorf("auth.Login: %v", err))
			return nil
		}
		if loginReply.Status != auth.Status_OK {
			log.Printf("Failed authentication of user %s", m.Login.Name)
			c.send(&pb.ServerMessage{Type: pb.ServerMessage_AUTHENTICATE})
			return nil
		}
		log.Printf("Successful authentication of user %s (%s)", m.Login.Name, loginReply.Id)
		c.user = loginReply.Id
		c.state = stateAuthenticated
		return nil
	default:
		return errors.New("unknown message type received")
	}
}

func (c *Connection) handleAuthenticatedMessage(m *pb.ClientMessage) error {
	switch m.Type {
	case pb.ClientMessage_GAME_COMMAND:
		log.Printf("Received game command %s", proto.CompactTextString(m))
		if c.s.GameCommandHandler == nil {
			return errors.New("game command handler is not installed")
		}
		return c.s.GameCommandHandler(c, m.Data)
	case pb.ClientMessage_LOCATION_COMMAND:
		log.Printf("Received location command %s", proto.CompactTextString(m))
		if c.loc == nil {
			return errors.New("no connection to the location server yet")
		}
		if m.Location == nil || c.loc.Type != m.Location.Type || c.loc.ID != m.Location.Id {
			return errors.New("location command sent to a wrong location")
		}
		_, err := c.loc.Client.ExecuteCommand(c.ctx, &gstp.ExecuteCommandRequest{Object: fmt.Sprintf("%s-%s", c.loc.Type, c.loc.ID), User: c.user, Data: m.Data})
		return err
	default:
		return errors.New("unknown message type received")
	}
}
