package arangodb

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/jalapeno/ipv4-topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) peerHandler(obj *kafkanotifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// todo: unicast prefix edge removal not working properly (january 20, 2022)

	glog.V(5).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.PeerStateChange
	_, err := a.peer.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a UnicastPrefix removal notification, reading it will return Not Found error
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
	case "update":
		glog.V(5).Infof("Send update msg to processEPEPrefix function")
		if err := a.processPeer(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for edge %s with error: %+v", obj.Action, obj.Key, err)
		}
	case "add":
		glog.V(5).Infof("Send add msg to processEPEPrefix function")
		if err := a.processPeer(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for edge %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
		// NOOP
	}

	return nil
}

// processEdge processes a unicast prefix connection which is a unidirectional edge between an eBGP peer and LSNode
func (a *arangoDB) processEBGPPeer(ctx context.Context, key string, e *LSNodeExt) error {
	query := "for l in peer" +
		" filter l.local_ip !like " + "\"%:%\"" +
		" filter l.local_asn != l.remote_asn " +
		" filter l.local_bgp_id == " + "\"" + e.RouterID + "\""
	query += " return l	"
	glog.Infof("running query: %s", query)
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var p message.PeerStateChange
		peer, err := pcursor.ReadDocument(ctx, &p)
		if err != nil {
			if driver.IsNoMoreDocuments(err) {
				return err
			}
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}

		glog.Infof("ls node ext %s + to eBGP peer %s", p.Key, e.Key)
		ne := peerEdgeObject{
			Key:         e.RouterID + "_" + peer.ID.Key(),
			From:        e.ID,
			To:          peer.ID.String(),
			LocalBGPID:  e.RouterID,
			RemoteBGPID: p.RemoteBGPID,
			LocalIP:     p.LocalIP,
			RemoteIP:    p.RemoteIP,
			RemoteASN:   p.RemoteASN,
			Name:        e.Name,
		}

		if _, err := a.graph.CreateDocument(ctx, &ne); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, ne.Key, &ne); err != nil {
				return err
			}
		}
	}

	return nil
}

// processEdge processes a unicast prefix connection which is a unidirectional edge between an eBGP peer and LSNode
func (a *arangoDB) processPeer(ctx context.Context, key string, e *message.PeerStateChange) error {
	query := "for l in peer " +
		" filter l.local_ip !like " + "\"%:%\"" +
		" filter l.remote_ip == " + "\"" + e.LocalIP + "\""
	query += " return l	"
	glog.V(5).Infof("running query: %s", query)
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var pr message.PeerStateChange
		peer, err := pcursor.ReadDocument(ctx, &pr)
		if err != nil {
			if driver.IsNoMoreDocuments(err) {
				return err
			}
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}

		glog.V(5).Infof("local peer %s + to remote eBGP peer %s", e.Key, pr.Key)
		ne := peerEdgeObject{
			Key:         peer.ID.Key(),
			From:        e.ID,
			To:          pr.ID,
			LocalBGPID:  e.LocalBGPID,
			RemoteBGPID: e.RemoteBGPID,
			LocalIP:     e.LocalIP,
			RemoteIP:    e.RemoteIP,
			LocalASN:    e.LocalASN,
			RemoteASN:   e.RemoteASN,
			Name:        e.Name,
		}

		if _, err := a.graph.CreateDocument(ctx, &ne); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, ne.Key, &ne); err != nil {
				return err
			}
		}
	}

	return nil
}

// processPeerRemoval removes records from Edge collection which are referring to deleted UnicastPrefix
func (a *arangoDB) processPeerRemoval(ctx context.Context, id string) error {
	query := "FOR d IN " + a.graph.Name() +
		" filter d._to == " + "\"" + id + "\""
	query += " return d"
	glog.V(6).Infof("query to remove prefix edge: %s", query)
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()
	for {
		var nm unicastPrefixEdgeObject
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.graph.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}
