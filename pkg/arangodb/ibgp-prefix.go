package arangodb

import (
	"context"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

// processEdge processes a unicast prefix connection which is a unidirectional edge between an eBGP peer and LSNode
func (a *arangoDB) processIBGP(ctx context.Context, key string, e *LSNodeExt) error {
	query := "for l in unicast_prefix_v4" +
		" filter l.peer_ip == " + "\"" + e.RouterID + "\"" +
		" filter l.nexthop == l.peer_ip filter l.origin_as == Null filter l.prefix_len != 32 "
	query += " return l	"
	glog.Infof("running iBGP prefix query: %s", query)
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var up message.UnicastPrefix
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

		glog.Infof("ls node %s + unicastprefix %s", e.Key, up.Key)
		ne := unicastPrefixEdgeObject{
			Key:  mp.ID.Key(),
			From: e.ID,
			To:   up.ID,
			// To:        mp.ID.String(),
			Prefix:    up.Prefix,
			PrefixLen: up.PrefixLen,
			LocalIP:   e.RouterID,
			PeerIP:    up.PeerIP,
			BaseAttrs: up.BaseAttributes,
			PeerASN:   e.PeerASN,
			OriginAS:  up.OriginAS,
			Nexthop:   up.Nexthop,
			Labels:    up.Labels,
			Name:      e.Name,
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
