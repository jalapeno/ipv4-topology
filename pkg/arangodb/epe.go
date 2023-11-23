package arangodb

import (
	"context"
	"strconv"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/base"
	"github.com/sbezverk/gobmp/pkg/message"
)

// processEPE. processes a single EPE ls_link connection which is a unidirectional edge from egress node to external peer (vertices).
func (a *arangoDB) processEPE(ctx context.Context, key string, e *message.LSLink) error {
	if e.ProtocolID != base.BGP {
		return nil
	}
	if strings.Contains(e.Key, ":") {
		return nil
	}
	// find matching ls_node_extended entry to get EPE node. Note if an existing ls_node to peer entry exists it will be overwritten by EPE
	query := "for d in " + a.lsnodeExt.Name() +
		" filter d.router_id == " + "\"" + e.BGPRouterID + "\"" +
		" filter d.domain_id == " + strconv.Itoa(int(e.DomainID))
	query += " return d"
	glog.Infof("query lsnode for router id matching lslink igp_id: %+v bgp_router_id: %+v, local_link_ip: %+v", e.IGPRouterID, e.BGPRouterID, e.LocalLinkIP)
	glog.Infof("query: %+v", query)
	lcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}

	defer lcursor.Close()
	var ln LSNodeExt
	lm, err := lcursor.ReadDocument(ctx, &ln)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	//glog.Infof("ls_node: %+v to correlate with EPE ls_link: %+v", ln.Key, e.Key)

	// query = "for d in ebgp_session_v4" +
	// 	" filter d.remote_bgp_id == " + "\"" + e.BGPRemoteRouterID + "\"" +
	// 	" filter d.remote_ip == " + "\"" + e.RemoteLinkIP + "\""
	// query += " return d"
	query = "for d in ebgp_peer_v4" +
		" filter d.bgp_router_id == " + "\"" + e.BGPRemoteRouterID + "\""
	query += " return d"
	//glog.Infof("query peer collection for router id matching lslink bgp_remote_router_id: %+v and remotelinkIP: %+v", e.BGPRemoteRouterID, e.RemoteLinkIP)
	rcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer rcursor.Close()
	var rn message.PeerStateChange
	rm, err := rcursor.ReadDocument(ctx, &rn)
	if err != nil {
		if !driver.IsNoMoreDocuments(err) {
			return err
		}
	}
	//glog.Infof("From lsnode: %+v, aka %+v", ln.Key, ln.RouterID)
	//glog.Infof("To peer: %+v aka: %+v", rn.Key, rm.ID.Key())

	ne := epeEdgeObject{
		Key: key,
		//Key:             ln.RouterID + "_" + rm.ID.Key(), //key,
		From:            lm.ID.String(),
		To:              rm.ID.String(),
		ProtocolID:      e.ProtocolID,
		DomainID:        e.DomainID,
		LocalNodeName:   ln.Name,
		RemoteNodeName:  rn.Name,
		LocalLinkIP:     e.LocalLinkIP,
		RemoteLinkIP:    rn.RemoteIP,
		LocalNodeASN:    e.LocalNodeASN,
		RemoteNodeASN:   e.RemoteNodeASN,
		RemoteNodeBGPID: e.BGPRemoteRouterID,
		PeerNodeSID:     e.PeerNodeSID,
		PeerAdjSID:      e.PeerAdjSID,
		PeerSetSID:      e.PeerSetSID,
	}
	var doc driver.DocumentMeta
	if doc, err = a.graph.CreateDocument(ctx, &ne); err != nil {

		if !driver.IsConflict(err) {
			return err
		}
		// The document already exists, updating it with the latest info
		doc, err = a.graph.UpdateDocument(ctx, e.Key, &ne)
		if err != nil {
			return err
		}
	}
	glog.Infof("epe doc %s uploaded", doc)
	return nil
}
