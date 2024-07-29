package arangodb

import (
	"context"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) processPeerSession(ctx context.Context, key string, p *message.PeerStateChange) error {
	//glog.Infof("process bgp session: %s", p.Key)
	// get local node from ls_link entry
	ln, err := a.getPeer(ctx, p, true)
	if err != nil {
		glog.Errorf("processEdge failed to get local peer %s for link: %s with error: %+v", p.LocalBGPID, p.ID, err)
		return err
	}
	// get remote node from ebgp peer entry
	rn, err := a.getPeer(ctx, p, false)
	if err != nil {
		glog.Errorf("processEdge failed to get remote peer %s for link: %s with error: %+v", p.RemoteBGPID, p.ID, err)
		return err
	}
	if err := a.createPeerEdge(ctx, p, ln, rn); err != nil {
		glog.Errorf("processEdge failed to create Edge object with error: %+v", err)
		return err
	}
	//glog.V(9).Infof("processEdge completed processing lslink: %s for ls nodes: %s - %s", l.ID, ln.ID, rn.ID)
	return nil
}

func (a *arangoDB) getPeer(ctx context.Context, e *message.PeerStateChange, local bool) (bgpNode, error) {
	// Need to find ls_node object matching ls_link's IGP Router ID
	query := "FOR d IN " + a.bgpNode.Name()
	if local {
		//glog.Infof("get local node per session: %s, %s", e.LocalBGPID, e.ID)
		query += " filter d.bgp_router_id == " + "\"" + e.LocalBGPID + "\""
	} else {
		//glog.Infof("get remote node per session: %s, %v", e.RemoteBGPID, e.ID)
		query += " filter d.bgp_router_id == " + "\"" + e.RemoteBGPID + "\""
	}
	query += " return d"
	//glog.Infof("query: %+v", query)
	lcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
	}
	defer lcursor.Close()
	var ln bgpNode
	i := 0
	for ; ; i++ {
		_, err := lcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
			}
			break
		}
	}
	if i == 0 {
		glog.Errorf("query %s returned 0 results", query)
	}
	if i > 1 {
		glog.Errorf("query %s returned more than 1 result", query)
	}
	return ln, nil
}

func (a *arangoDB) createPeerEdge(ctx context.Context, l *message.PeerStateChange, ln, rn bgpNode) error {

	pf := peerFromObject{
		Key:       l.Key,
		From:      ln.ID,
		To:        rn.ID,
		LocalIP:   l.LocalIP,
		RemoteIP:  l.RemoteIP,
		LocalASN:  l.LocalASN,
		RemoteASN: l.RemoteASN,
	}
	if _, err := a.graph.CreateDocument(ctx, &pf); err != nil {
		if !driver.IsConflict(err) {
			return err
		}
		// The document already exists, updating it with the latest info
		if _, err := a.graph.UpdateDocument(ctx, pf.Key, &pf); err != nil {
			return err
		}
	}
	return nil
}

func (a *arangoDB) processEgressPeer(ctx context.Context, key string, p *message.PeerStateChange) error {

	glog.Infof("process ebgp session: %s", p.Key)
	// get local node from ls_link entry
	ln, err := a.getLocalnode(ctx, p, true)
	if err != nil {
		glog.Errorf("processEdge failed to get local peer %s for link: %s with error: %+v", p.LocalBGPID, p.ID, err)
		return err
	}

	// get remote node from ebgp peer entry
	rn, err := a.getExtPeer(ctx, p, false)
	if err != nil {
		glog.Errorf("processEdge failed to get remote peer %s for link: %s with error: %+v", p.RemoteBGPID, p.ID, err)
		return err
	}
	if err := a.createPRedge(ctx, p, ln, rn); err != nil {
		glog.Errorf("processEdge failed to create Edge object with error: %+v", err)
		return err
	}
	//glog.V(9).Infof("processEdge completed processing lslink: %s for ls nodes: %s - %s", l.ID, ln.ID, rn.ID)
	return nil
}

func (a *arangoDB) getLocalnode(ctx context.Context, e *message.PeerStateChange, local bool) (LSNodeExt, error) {
	// Need to find ls_node object matching ls_link's IGP Router ID
	query := "FOR d IN " + a.lsnodeExt.Name()
	if local {
		glog.Infof("get local node per session: %s, %s", e.LocalBGPID, e.ID)
		query += " filter d.router_id == " + "\"" + e.LocalBGPID + "\""
	} else {
		glog.Infof("get remote node per session: %s, %v", e.RemoteBGPID, e.ID)
		query += " filter d.router_id == " + "\"" + e.RemoteBGPID + "\""
	}
	query += " return d"
	//glog.Infof("query: %+v", query)
	lcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
	}
	defer lcursor.Close()
	var ln LSNodeExt
	i := 0
	for ; ; i++ {
		_, err := lcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
			}
			break
		}
	}
	if i == 0 {
		glog.Errorf("query %s returned 0 results", query)
	}
	if i > 1 {
		glog.Errorf("query %s returned more than 1 result", query)
	}
	return ln, nil
}

func (a *arangoDB) getExtPeer(ctx context.Context, e *message.PeerStateChange, local bool) (bgpNode, error) {
	// Need to find ls_node object matching ls_link's IGP Router ID
	query := "FOR d IN " + a.bgpNode.Name()
	if local {
		glog.Infof("get local node per session: %s, %s", e.LocalBGPID, e.ID)
		query += " filter d.bgp_router_id == " + "\"" + e.LocalBGPID + "\""
	} else {
		glog.Infof("get remote node per session: %s, %v", e.RemoteBGPID, e.ID)
		query += " filter d.bgp_router_id == " + "\"" + e.RemoteBGPID + "\""
	}
	query += " return d"
	//glog.Infof("query: %+v", query)
	lcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
	}
	defer lcursor.Close()
	var ln bgpNode
	i := 0
	for ; ; i++ {
		_, err := lcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				glog.Errorf("failed to process key: %s with error: %+v", e.Key, err)
			}
			break
		}
	}
	if i == 0 {
		glog.Errorf("query %s returned 0 results", query)
	}
	if i > 1 {
		glog.Errorf("query %s returned more than 1 result", query)
	}
	return ln, nil
}

func (a *arangoDB) createPRedge(ctx context.Context, p *message.PeerStateChange, ln LSNodeExt, rn bgpNode) error {

	if p.LocalASN == p.RemoteASN {
		glog.Infof("peer message is iBGP, no further processing")
	} else {
		pf := peerToObject{
			Key:       ln.Key + "_" + p.Key,
			From:      ln.ID,
			To:        rn.ID,
			LocalIP:   p.LocalIP,
			RemoteIP:  p.RemoteIP,
			LocalASN:  p.LocalASN,
			RemoteASN: p.RemoteASN,
		}
		if _, err := a.graph.CreateDocument(ctx, &pf); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, pf.Key, &pf); err != nil {
				return err
			}
		}
		pt := peerFromObject{
			Key:       rn.Key + "_" + p.Key,
			From:      rn.ID,
			To:        ln.ID,
			Session:   p.Key,
			LocalIP:   p.LocalIP,
			RemoteIP:  p.RemoteIP,
			LocalASN:  p.LocalASN,
			RemoteASN: p.RemoteASN,
		}
		if _, err := a.graph.CreateDocument(ctx, &pt); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.graph.UpdateDocument(ctx, pt.Key, &pt); err != nil {
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
