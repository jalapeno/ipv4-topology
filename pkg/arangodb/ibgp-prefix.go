package arangodb

import (
	"context"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) processIBGPPrefix(ctx context.Context, key string, e *LSNodeExt) error {
	query := "for l in unicast_prefix_v4" +
		" filter l.peer_ip == " + "\"" + e.RouterID + "\"" +
		" filter l.nexthop == l.peer_ip filter l.origin_as == Null " +
		" filter l.prefix_len != 32 filter l.prefix != " + "\"0.0.0.0\""
	query += " return l	"
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

		glog.Infof("ls node %s + iBGP prefix %s, meta %+v", e.Key, up.Key, mp)
		from := unicastPrefixEdgeObject{
			Key:       up.Key + "_" + e.Key,
			From:      up.ID,
			To:        e.ID,
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
			LocalIP:   e.RouterID,
			PeerIP:    up.PeerIP,
			BaseAttrs: up.BaseAttributes,
			PeerASN:   e.PeerASN,
			OriginAS:  up.OriginAS,
			Nexthop:   up.Nexthop,
			Labels:    up.Labels,
			Name:      e.Name,
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
