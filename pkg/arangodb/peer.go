package arangodb

import (
	"context"
	"fmt"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/jalapeno/ipv4-topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) peerHandler(obj *kafkanotifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// Check if Collection encoded in ID exists
	c := strings.Split(obj.ID, "/")[0]
	if strings.Compare(c, a.peer.Name()) != 0 {
		return fmt.Errorf("configured collection name %s and received in event collection name %s do not match", a.peer.Name(), c)
	}
	//glog.V(5).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.PeerStateChange
	_, err := a.peer.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a BGP Peer removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processPeerRemoval(ctx, obj.ID)
	}
	switch obj.Action {
	case "add":
		if err := a.processEdgeByPeer(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for vertex %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
		// NOOP for update
	}

	return nil
}
