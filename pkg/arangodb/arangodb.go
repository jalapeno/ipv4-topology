package arangodb

import (
	"context"
	"encoding/json"

	driver "github.com/arangodb/go-driver"
	"github.com/cisco-open/jalapeno/topology/dbclient"
	"github.com/golang/glog"
	"github.com/jalapeno/ipv4-topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/tools"
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop          chan struct{}
	lslink        driver.Collection
	lsprefix      driver.Collection
	graph         driver.Collection
	lsnodeExt     driver.Collection
	unicastprefix driver.Collection
	peer          driver.Collection
	inetPrefix    driver.Collection
	lstopoV4      driver.Graph
	ipv4topo      driver.Graph
	notifier      kafkanotifier.Event
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, lslink string, lsprefix string, lsnodeExt string,
	unicastprefix string, peer string, inetPrefix string, lstopoV4 string, ipv4topo string, notifier kafkanotifier.Event) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop: make(chan struct{}),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn
	if notifier != nil {
		arango.notifier = notifier
	}

	// Check if ls_link edge collection exists, if not fail as Jalapeno topology is not running
	arango.lslink, err = arango.db.Collection(context.TODO(), lslink)
	if err != nil {
		return nil, err
	}

	// Check if ls_prefix collection exists, if not fail as Jalapeno topology is not running
	arango.lsprefix, err = arango.db.Collection(context.TODO(), lsprefix)
	if err != nil {
		return nil, err
	}

	//Check if ls_node_ext collection exists, if not fail as Jalapeno topology is not running
	arango.lsnodeExt, err = arango.db.Collection(context.TODO(), lsnodeExt)
	if err != nil {
		return nil, err
	}

	// Check if original ls_node collection exists, if not fail as Jalapeno topology is not running
	arango.unicastprefix, err = arango.db.Collection(context.TODO(), unicastprefix)
	if err != nil {
		return nil, err
	}

	// Check if original ls_node collection exists, if not fail as Jalapeno topology is not running
	arango.peer, err = arango.db.Collection(context.TODO(), peer)
	if err != nil {
		return nil, err
	}

	// Check if inet_prefix collection exists, if not fail as Jalapeno topology is not running
	arango.inetPrefix, err = arango.db.Collection(context.TODO(), inetPrefix)
	if err != nil {
		return nil, err
	}

	// Check if original ls_topology collection exists, if not fail as Jalapeno topology is not running
	arango.lstopoV4, err = arango.db.Graph(context.TODO(), lstopoV4)
	glog.Infof("lsv4 topo collection found %+v", lstopoV4)
	if err != nil {
		return nil, err
	}

	// check for lsnode_extended collection
	found, err := arango.db.CollectionExists(context.TODO(), lsnodeExt)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), lsnodeExt)
		if err != nil {
			return nil, err
		}
		glog.Infof("ls_node_extended collection found %s, proceed to processing data", c)
	}

	// check for ipv4 topology graph
	found, err = arango.db.GraphExists(context.TODO(), ipv4topo)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Graph(context.TODO(), ipv4topo)
		if err != nil {
			return nil, err
		}
		glog.Infof("found graph %s", c)

	} else {
		// create graph
		var edgeDefinition driver.EdgeDefinition
		edgeDefinition.Collection = "ipv4_topology"
		edgeDefinition.From = []string{"ls_node_extended", "ls_prefix", "peer", "unicast_prefix_v4"}
		edgeDefinition.To = []string{"ls_node_extended", "ls_prefix", "peer", "unicast_prefix_v4"}
		var options driver.CreateGraphOptions
		//options.OrphanVertexCollections = []string{"ls_srv6_sid", "ls_prefix"}
		options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}

		arango.ipv4topo, err = arango.db.CreateGraph(context.TODO(), ipv4topo, &options)
		if err != nil {
			return nil, err
		}
	}

	// check if graph exists, if not fail as processor has failed to create graph
	arango.graph, err = arango.db.Collection(context.TODO(), "ipv4_topology")
	if err != nil {
		return nil, err
	}
	return arango, nil
}

func (a *arangoDB) Start() error {
	if err := a.loadEdge(); err != nil {
		return err
	}
	glog.Infof("Connected to arango database, starting monitor")

	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType dbclient.CollectionType, msg []byte) error {
	event := &kafkanotifier.EventMessage{}
	if err := json.Unmarshal(msg, event); err != nil {
		return err
	}
	glog.V(9).Infof("Received event from topology: %+v", *event)
	event.TopicType = msgType
	switch msgType {
	case bmp.LSLinkMsg:
		return a.lsLinkHandler(event)
	}
	switch msgType {
	case bmp.LSPrefixMsg:
		return a.lsprefixHandler(event)
	}
	switch msgType {
	case bmp.PeerStateChangeMsg:
		return a.peerHandler(event)
	}
	switch msgType {
	case bmp.UnicastPrefixV4Msg:
		return a.unicastprefixHandler(event)
	}
	return nil
}

func (a *arangoDB) loadEdge() error {
	ctx := context.TODO()

	copy_ls_topo := "for l in ls_topology_v4 insert l in ipv4_topology options { overwrite: " + "\"update\"" + " } "
	cursor, err := a.db.Query(ctx, copy_ls_topo, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()

	ebgp_query := "for l in " + a.lsnodeExt.Name() + " return l"
	cursor, err = a.db.Query(ctx, ebgp_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p LSNodeExt
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("processing eBGP peers for ls_node: %s", p.Key)
		if err := a.processEBGPPeer(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	epe_query := "for l in ls_link filter l.protocol_id == 7 filter l._key !like " + "\"%:%\"" + " return l"
	cursor, err = a.db.Query(ctx, epe_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.LSLink
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv4 epe_link: %s", p.Key)
		if err := a.processEPE(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	ibgp_prefix_query := "for l in " + a.lsnodeExt.Name() + " return l"
	cursor, err = a.db.Query(ctx, ibgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p LSNodeExt
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv4 iBGP prefixes attach to lsnode: %s", p.Key)
		if err := a.processIBGP(ctx, meta.Key, &p); err != nil {
			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	ebgp_prefix_query := "for l in unicast_prefix_v4  " +
		"filter l.prefix_len < 25 filter l.base_attrs.local_pref == null return l"
	cursor, err = a.db.Query(ctx, ebgp_prefix_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.UnicastPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		//glog.Infof("get ipv4 eBGP prefixes: %s", p.Key)
		if err := a.processUnicastPrefix(ctx, meta.Key, &p); err != nil {
			//if err := a.processUnicastPrefix(ctx, meta.Key, &p); err != nil {

			glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
			continue
		}
	}

	// peer2peer_query := "for l in " + a.peer.Name() + " return l"
	// cursor, err = a.db.Query(ctx, peer2peer_query, nil)
	// if err != nil {
	// 	return err
	// }
	// defer cursor.Close()
	// for {
	// 	var p message.PeerStateChange
	// 	meta, err := cursor.ReadDocument(ctx, &p)
	// 	if driver.IsNoMoreDocuments(err) {
	// 		break
	// 	} else if err != nil {
	// 		return err
	// 	}
	// 	glog.V(5).Infof("connect eBGP peers in graph: %s", p.Key)
	// 	if err := a.processPeer(ctx, meta.Key, &p); err != nil {
	// 		glog.Errorf("failed to process key: %s with error: %+v", meta.Key, err)
	// 		continue
	// 	}
	// }

	return nil
}
