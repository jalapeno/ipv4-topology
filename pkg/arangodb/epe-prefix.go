package arangodb

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/jalapeno/ipv4-topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) unicastprefixHandler(obj *kafkanotifier.EventMessage) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("event message is nil")
	}
	// todo: unicast prefix edge removal not working properly (january 20, 2022)

	glog.V(5).Infof("Processing action: %s for key: %s ID: %s", obj.Action, obj.Key, obj.ID)
	var o message.UnicastPrefix
	_, err := a.unicastprefix.ReadDocument(ctx, obj.Key, &o)
	if err != nil {
		// In case of a UnicastPrefix removal notification, reading it will return Not Found error
		if !driver.IsNotFound(err) {
			return fmt.Errorf("failed to read existing document %s with error: %+v", obj.Key, err)
		}
		// If operation matches to "del" then it is confirmed delete operation, otherwise return error
		if obj.Action != "del" {
			return fmt.Errorf("document %s not found but Action is not \"del\", possible stale event", obj.Key)
		}
		return a.processUnicastPrefixRemoval(ctx, obj.ID)
	}
	switch obj.Action {
	case "update":
		glog.V(5).Infof("Send update msg to processEPEPrefix function")
		if err := a.processEPEPrefix(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for edge %s with error: %+v", obj.Action, obj.Key, err)
		}
	case "add":
		glog.V(5).Infof("Send add msg to processEPEPrefix function")
		if err := a.processEPEPrefix(ctx, obj.Key, &o); err != nil {
			return fmt.Errorf("failed to process action %s for edge %s with error: %+v", obj.Action, obj.Key, err)
		}
	default:
		// NOOP
	}

	return nil
}

// processEdge processes a single ls_link connection which is a unidirectional edge between two ls_nodes (vertices).
func (a *arangoDB) processEPEPrefix(ctx context.Context, key string, e *message.UnicastPrefix) error {
	if e.BaseAttributes.ASPath == nil {
		glog.V(5).Infof("running filtered query: %s", e.Key)
		return a.processInternalPrefix(ctx, key, e)
	}
	query := "FOR d IN " + a.peer.Name() +
		" filter d.remote_ip == " + "\"" + e.PeerIP + "\"" +
		" FOR l in ls_link " +
		" filter d.remote_ip == l.remote_link_ip "
	query += " return d"
	glog.V(5).Infof("running filtered query: %s", query)
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()
	var nm message.PeerStateChange
	mn, err := ncursor.ReadDocument(ctx, &nm)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}

	glog.V(5).Infof("peer %s + prefix %s", nm.Key, e.Key)

	ne := unicastPrefixEdgeObject{
		Key:       key,
		From:      mn.ID.String(),
		To:        e.ID,
		Prefix:    e.Prefix,
		PrefixLen: e.PrefixLen,
		LocalIP:   e.RouterIP,
		PeerIP:    e.PeerIP,
		BaseAttrs: e.BaseAttributes,
		PeerASN:   e.PeerASN,
		OriginAS:  e.OriginAS,
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

	return nil
}

func (a *arangoDB) processEdgeByPeer(ctx context.Context, key string, e *message.PeerStateChange) error {
	if e.LocalASN == e.RemoteASN {
		//return a.processIBGP(ctx, key, e)
		glog.V(5).Infof("local as: %s, remote as: %s", e.LocalASN, e.RemoteASN)
		return nil
	}
	query := "FOR d IN unicast_prefix_v4" + //a.unicastprefixv4.Name() +
		" FOR l in ls_link " +
		" filter d.peer_ip == l.remote_link_ip " +
		" filter d.peer_ip == " + "\"" + e.RemoteIP + "\""
	query += " return d	"
	glog.V(5).Infof("running query: %s", query)
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var pm message.UnicastPrefix
		mp, err := pcursor.ReadDocument(ctx, &pm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}

		glog.V(6).Infof("peer %s + unicastprefix %s", e.Key, pm.Key)
		ne := unicastPrefixEdgeObject{
			Key:       mp.ID.Key(),
			From:      e.ID,
			To:        mp.ID.String(),
			Prefix:    pm.Prefix,
			PrefixLen: pm.PrefixLen,
			LocalIP:   pm.RouterIP,
			PeerIP:    pm.PeerIP,
			BaseAttrs: pm.BaseAttributes,
			PeerASN:   pm.PeerASN,
			OriginAS:  pm.OriginAS,
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

// processPrefixRemoval removes records from Edge collection which are referring to deleted UnicastPrefix
func (a *arangoDB) processUnicastPrefixRemoval(ctx context.Context, id string) error {
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

// processPeerRemoval removes records from Edge collection which are referring to deleted eBGP Peer
func (a *arangoDB) processPeerRemoval(ctx context.Context, id string) error {
	query := "FOR d IN " + a.graph.Name() +
		" filter d._from == " + "\"" + id + "\""
	query += " return d"
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
