package arangodb

import (
	"context"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) processInetPrefix(ctx context.Context, key string, e *message.UnicastPrefix) error {
	query := "for l in ebgp_peer_v4 filter l.asn !in 64512..65535" //+
	//" filter l.asn == " + strconv.Itoa(int(e.OriginAS))
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

		glog.Infof("ebgp peer %s + eBGP prefix %s, meta %+v", e.Key, up.Key, mp)
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

func (a *arangoDB) processeBgpPrefix(ctx context.Context, key string, e *message.UnicastPrefix) error {
	// get internal ASN so we can determine whether this is an external prefix or not
	getasn := "for l in ls_node_extended limit 1 return l"
	cursor, err := a.db.Query(ctx, getasn, nil)
	if err != nil {
		return err
	}

	var ln LSNodeExt
	lm, err := cursor.ReadDocument(ctx, &ln)
	glog.Infof("meta %+v", lm)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}

	if e.PeerASN == ln.ASN {
		glog.Infof("external prefix learned from internal BMP node, do not process: %+v, peer_as: %+v, ln.ASN: %+v", e.Prefix, e.PeerASN, ln.ASN)
		return a.processUnicastPrefixRemoval(ctx, key)

	} else {
		query := "for l in ebgp_peer_v4 filter l.asn in 64512..65535" +
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
	}
	return nil
}

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
