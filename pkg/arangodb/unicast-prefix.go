package arangodb

import (
	"context"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

// processEdge processes a single ls_link connection which is a unidirectional edge between two nodes (vertices).
// func (a *arangoDB) processInetPrefix(ctx context.Context, key string, l *message.UnicastPrefix) error {
// 	// todo: process iBGP prefixes

// 	// if base.ProtoID(l.BaseAttributes.LocalPref) == nil {
// 	// 	return a.processIBGP(ctx, key, &l)
// 	// }
// 	glog.Infof("processEdge processing unicast prefix: %s", l.ID)
// 	ln, err := a.getInetPfx(ctx, l, true)
// 	if err != nil {
// 		glog.Errorf("processEdge failed to get inet prefix %s for unicast prefix: %s with error: %+v", l.Prefix, l.ID, err)
// 		return err
// 	}
// 	rn, err := a.getPeer(ctx, l, false)
// 	if err != nil {
// 		glog.Errorf("processEdge failed to get eBGP Peer %s for unicast prefix: %s with error: %+v", l.Nexthop, l.ID, err)
// 		return err
// 	}
// 	//glog.V(6).Infof("Local node -> Protocol: %+v Domain ID: %+v IGP Router ID: %+v", ln.ProtocolID, ln.DomainID, ln.IGPRouterID)
// 	//glog.V(6).Infof("Remote node -> Protocol: %+v Domain ID: %+v IGP Router ID: %+v", rn.ProtocolID, rn.DomainID, rn.IGPRouterID)
// 	if err := a.createInetObject(ctx, l, ln, rn); err != nil {
// 		glog.Errorf("processEdge failed to create Edge object with error: %+v", err)
// 		return err
// 	}
// 	return nil
// }

// func (a *arangoDB) getInetPfx(ctx context.Context, e *message.UnicastPrefix, local bool) (*inetPrefix, error) {
// 	// Need to find ls_node object matching ls_link's IGP Router ID
// 	query := "FOR d IN inet_prefix_v4 filter d.prefix == " + "\"" + e.Prefix + "\""
// 	query += " return d"
// 	glog.Infof("query: %+v", query)
// 	lcursor, err := a.db.Query(ctx, query, nil)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer lcursor.Close()
// 	var ln inetPrefix
// 	i := 0
// 	for ; ; i++ {
// 		_, err := lcursor.ReadDocument(ctx, &ln)
// 		if err != nil {
// 			if !driver.IsNoMoreDocuments(err) {
// 				return nil, err
// 			}
// 			break
// 		}
// 	}
// 	if i == 0 {
// 		return nil, fmt.Errorf("query %s returned 0 results", query)
// 	}
// 	if i > 1 {
// 		return nil, fmt.Errorf("query %s returned more than 1 result", query)
// 	}

// 	return &ln, nil
// }

// func (a *arangoDB) getPeer(ctx context.Context, e *message.UnicastPrefix, local bool) (*message.PeerStateChange, error) {
// 	// Need to find peer object matching unicast prefix's nexthop
// 	query := "FOR d IN peer filter d.remote_ip == " + "\"" + e.Nexthop + "\""
// 	query += " return d"
// 	//glog.Infof("query: %s", query)
// 	lcursor, err := a.db.Query(ctx, query, nil)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer lcursor.Close()
// 	var rn message.PeerStateChange
// 	i := 0
// 	for ; ; i++ {
// 		_, err := lcursor.ReadDocument(ctx, &rn)
// 		if err != nil {
// 			if !driver.IsNoMoreDocuments(err) {
// 				return nil, err
// 			}
// 			break
// 		}
// 	}
// 	if i == 0 {
// 		return nil, fmt.Errorf("query %s returned 0 results", query)
// 	}
// 	if i > 1 {
// 		return nil, fmt.Errorf("query %s returned more than 1 result", query)
// 	}

// 	return &rn, nil
// }

// func (a *arangoDB) createInetObject(ctx context.Context, l *message.UnicastPrefix, ln *inetPrefix, rn *message.PeerStateChange) error {

// 	ne := inetPrefixEdgeObject{
// 		Key:      l.Key,
// 		From:     ln.ID,
// 		To:       rn.ID,
// 		OriginAS: l.OriginAS,
// 	}
// 	if _, err := a.graph.CreateDocument(ctx, &ne); err != nil {
// 		if !driver.IsConflict(err) {
// 			return err
// 		}
// 		// The document already exists, updating it with the latest info
// 		if _, err := a.graph.UpdateDocument(ctx, ne.Key, &ne); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// processEdgeRemoval removes a record from Node's graph collection
// since the key matches in both collections (LS Links and Nodes' Graph) deleting the record directly.
func (a *arangoDB) processUnicastPrefixRemoval(ctx context.Context, key string) error {
	if _, err := a.graph.RemoveDocument(ctx, key); err != nil {
		if !driver.IsNotFound(err) {
			return err
		}
		return nil
	}

	return nil
}

func (a *arangoDB) processInetPrefix(ctx context.Context, key string, e *message.UnicastPrefix) error {
	query := "for l in ebgp_peer_v4" +
		" filter l.asn == " + strconv.Itoa(int(e.OriginAS))
	query += " return l	"
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var up inetPrefix
		mp, err := pcursor.ReadDocument(ctx, &up)
		if err != nil {
			if driver.IsNoMoreDocuments(err) {
				return err
			}
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}

		glog.Infof("ls node %s + iBGP prefix %s, meta %+v", e.Key, up.Key, mp)
		from := unicastPrefixEdgeObject{
			Key:       up.Key + "_" + e.Key,
			From:      up.ID,
			To:        e.ID,
			Prefix:    up.Prefix,
			PrefixLen: up.PrefixLen,
			OriginAS:  up.OriginAS,
		}

		if _, err := a.graph.CreateDocument(ctx, &from); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, from.Key, &from); err != nil {
				return err
			}
		}
		to := unicastPrefixEdgeObject{
			Key:       e.Key + "_" + up.Key,
			From:      e.ID,
			To:        up.ID,
			Prefix:    up.Prefix,
			PrefixLen: up.PrefixLen,
			OriginAS:  up.OriginAS,
		}

		if _, err := a.graph.CreateDocument(ctx, &to); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, to.Key, &to); err != nil {
				return err
			}
		}
	}

	return nil
}
