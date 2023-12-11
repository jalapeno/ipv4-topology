package arangodb

import (
	"github.com/sbezverk/gobmp/pkg/base"
	"github.com/sbezverk/gobmp/pkg/bgp"
	"github.com/sbezverk/gobmp/pkg/bgpls"
	"github.com/sbezverk/gobmp/pkg/sr"
	"github.com/sbezverk/gobmp/pkg/srv6"
)

type lsTopologyObject struct {
	Key                   string                `json:"_key"`
	From                  string                `json:"_from"`
	To                    string                `json:"_to"`
	Link                  string                `json:"link"`
	ProtocolID            base.ProtoID          `json:"protocol_id"`
	DomainID              int64                 `json:"domain_id"`
	MTID                  uint16                `json:"mt_id"`
	AreaID                string                `json:"area_id"`
	Protocol              string                `json:"protocol"`
	LocalLinkID           uint32                `json:"local_link_id"`
	RemoteLinkID          uint32                `json:"remote_link_id"`
	LocalLinkIP           string                `json:"local_link_ip"`
	RemoteLinkIP          string                `json:"remote_link_ip"`
	LocalNodeASN          uint32                `json:"local_node_asn"`
	RemoteNodeASN         uint32                `json:"remote_node_asn"`
	PeerNodeSID           *sr.PeerSID           `json:"peer_node_sid,omitempty"`
	PeerAdjSID            *sr.PeerSID           `json:"peer_adj_sid,omitempty"`
	PeerSetSID            *sr.PeerSID           `json:"peer_set_sid,omitempty"`
	SRv6BGPPeerNodeSID    *srv6.BGPPeerNodeSID  `json:"srv6_bgp_peer_node_sid,omitempty"`
	SRv6ENDXSID           []*srv6.EndXSIDTLV    `json:"srv6_endx_sid,omitempty"`
	LSAdjacencySID        []*sr.AdjacencySIDTLV `json:"ls_adjacency_sid,omitempty"`
	UnidirLinkDelay       uint32                `json:"unidir_link_delay"`
	UnidirLinkDelayMinMax []uint32              `json:"unidir_link_delay_min_max"`
	UnidirDelayVariation  uint32                `json:"unidir_delay_variation,omitempty"`
	UnidirPacketLoss      uint32                `json:"unidir_packet_loss,omitempty"`
	UnidirResidualBW      uint32                `json:"unidir_residual_bw,omitempty"`
	UnidirAvailableBW     uint32                `json:"unidir_available_bw,omitempty"`
	UnidirBWUtilization   uint32                `json:"unidir_bw_utilization,omitempty"`
	Prefix                string                `json:"prefix"`
	PrefixLen             int32                 `json:"prefix_len"`
	PrefixMetric          uint32                `json:"prefix_metric"`
	PrefixAttrTLVs        *bgpls.PrefixAttrTLVs `json:"prefix_attr_tlvs"`
}

type LSNodeExt struct {
	Key                  string                          `json:"_key,omitempty"`
	ID                   string                          `json:"_id,omitempty"`
	Rev                  string                          `json:"_rev,omitempty"`
	Action               string                          `json:"action,omitempty"` // Action can be "add" or "del"
	Sequence             int                             `json:"sequence,omitempty"`
	Hash                 string                          `json:"hash,omitempty"`
	RouterHash           string                          `json:"router_hash,omitempty"`
	DomainID             int64                           `json:"domain_id"`
	RouterIP             string                          `json:"router_ip,omitempty"`
	PeerHash             string                          `json:"peer_hash,omitempty"`
	PeerIP               string                          `json:"peer_ip,omitempty"`
	PeerASN              uint32                          `json:"peer_asn,omitempty"`
	Timestamp            string                          `json:"timestamp,omitempty"`
	IGPRouterID          string                          `json:"igp_router_id,omitempty"`
	RouterID             string                          `json:"router_id,omitempty"`
	ASN                  uint32                          `json:"asn,omitempty"`
	LSID                 uint32                          `json:"ls_id,omitempty"`
	MTID                 []*base.MultiTopologyIdentifier `json:"mt_id_tlv,omitempty"`
	AreaID               string                          `json:"area_id"`
	Protocol             string                          `json:"protocol,omitempty"`
	ProtocolID           base.ProtoID                    `json:"protocol_id,omitempty"`
	NodeFlags            *bgpls.NodeAttrFlags            `json:"node_flags,omitempty"`
	Name                 string                          `json:"name,omitempty"`
	SRCapabilities       *sr.Capability                  `json:"ls_sr_capabilities,omitempty"`
	SRAlgorithm          []int                           `json:"sr_algorithm,omitempty"`
	SRLocalBlock         *sr.LocalBlock                  `json:"sr_local_block,omitempty"`
	SRv6CapabilitiesTLV  *srv6.CapabilityTLV             `json:"srv6_capabilities_tlv,omitempty"`
	NodeMSD              []*base.MSDTV                   `json:"node_msd,omitempty"`
	FlexAlgoDefinition   []*bgpls.FlexAlgoDefinition     `json:"flex_algo_definition,omitempty"`
	IsPrepolicy          bool                            `json:"is_prepolicy"`
	IsAdjRIBIn           bool                            `json:"is_adj_rib_in"`
	Prefix               string                          `json:"prefix,omitempty"`
	PrefixLen            int32                           `json:"prefix_len,omitempty"`
	PrefixAttrTLVs       *bgpls.PrefixAttrTLVs           `json:"prefix_attr_tlvs,omitempty"`
	PrefixSID            []*sr.PrefixSIDTLV              `json:"prefix_sid_tlv,omitempty"`
	FlexAlgoPrefixMetric []*bgpls.FlexAlgoPrefixMetric   `json:"flex_algo_prefix_metric,omitempty"`
	SRv6SID              string                          `json:"srv6_sid,omitempty"`
	SIDS                 []*SID                          `json:"sids,omitempty"`
}

type SID struct {
	SRv6SID              string                 `json:"srv6_sid,omitempty"`
	SRv6EndpointBehavior *srv6.EndpointBehavior `json:"srv6_endpoint_behavior,omitempty"`
	SRv6BGPPeerNodeSID   *srv6.BGPPeerNodeSID   `json:"srv6_bgp_peer_node_sid,omitempty"`
	SRv6SIDStructure     *srv6.SIDStructure     `json:"srv6_sid_structure,omitempty"`
}

type epeEdgeObject struct {
	Key             string                `json:"_key"`
	From            string                `json:"_from"`
	To              string                `json:"_to"`
	ProtocolID      base.ProtoID          `json:"protocol_id"`
	DomainID        int64                 `json:"domain_id"`
	MTID            uint16                `json:"mt_id"`
	LocalNodeName   string                `json:"local_node_name"`
	RemoteNodeName  string                `json:"remote_node_name"`
	LocalLinkIP     string                `json:"local_link_ip"`
	RemoteLinkIP    string                `json:"remote_link_ip"`
	LocalNodeASN    uint32                `json:"local_node_asn"`
	RemoteNodeASN   uint32                `json:"remote_node_asn"`
	SRv6ENDXSID     []*srv6.EndXSIDTLV    `json:"srv6_endx_sid"`
	LSAdjSID        []*sr.AdjacencySIDTLV `json:"ls_adj_sid"`
	RemoteNodeBGPID string                `json:"bgp_remote_router_id"`
	PeerNodeSID     *sr.PeerSID           `json:"peer_node_sid"`
	PeerAdjSID      *sr.PeerSID           `json:"peer_adj_sid"`
	PeerSetSID      *sr.PeerSID           `json:"peer_set_sid"`
}

type peerToObject struct {
	Key         string              `json:"_key"`
	From        string              `json:"_from"`
	To          string              `json:"_to"`
	LocalBGPID  string              `json:"local_bgp_id"`
	RemoteBGPID string              `json:"remote_bgp_id"`
	LocalIP     string              `json:"local_ip"`
	RemoteIP    string              `json:"remote_ip"`
	BaseAttrs   *bgp.BaseAttributes `json:"base_attrs"`
	LocalASN    uint32              `json:"local_asn"`
	RemoteASN   uint32              `json:"reote_asn"`
	OriginAS    int32               `json:"origin_as"`
	ProtocolID  base.ProtoID        `json:"protocol_id"`
	Nexthop     string              `json:"nexthop"`
	Labels      []uint32            `json:"labels"`
	Name        string              `json:"name"`
	Session     string              `json:"session"`
}

type peerFromObject struct {
	Key         string              `json:"_key"`
	From        string              `json:"_from"`
	To          string              `json:"_to"`
	LocalBGPID  string              `json:"local_bgp_id"`
	RemoteBGPID string              `json:"remote_bgp_id"`
	LocalIP     string              `json:"local_ip"`
	RemoteIP    string              `json:"remote_ip"`
	BaseAttrs   *bgp.BaseAttributes `json:"base_attrs"`
	LocalASN    uint32              `json:"local_asn"`
	RemoteASN   uint32              `json:"reote_asn"`
	OriginAS    int32               `json:"origin_as"`
	ProtocolID  base.ProtoID        `json:"protocol_id"`
	Nexthop     string              `json:"nexthop"`
	Labels      []uint32            `json:"labels"`
	Name        string              `json:"name"`
	Session     string              `json:"session"`
}

type unicastPrefixEdgeObject struct {
	Key        string              `json:"_key"`
	From       string              `json:"_from"`
	To         string              `json:"_to"`
	Prefix     string              `json:"prefix"`
	PrefixLen  int32               `json:"prefix_len"`
	LocalIP    string              `json:"router_ip"`
	PeerIP     string              `json:"peer_ip"`
	BaseAttrs  *bgp.BaseAttributes `json:"base_attrs"`
	PeerASN    uint32              `json:"peer_asn"`
	OriginAS   int32               `json:"origin_as"`
	ProtocolID base.ProtoID        `json:"protocol_id"`
	Nexthop    string              `json:"nexthop"`
	Labels     []uint32            `json:"labels"`
	Name       string              `json:"name"`
	PeerName   string              `json:"peer_name"`
}

type inetPrefix struct {
	ID        string `json:"_id,omitempty"`
	Key       string `json:"_key,omitempty"`
	Prefix    string `json:"prefix,omitempty"`
	PrefixLen int32  `json:"prefix_len,omitempty"`
	OriginAS  int32  `json:"origin_as"`
}

type inetPrefixEdgeObject struct {
	Key        string              `json:"_key"`
	From       string              `json:"_from"`
	To         string              `json:"_to"`
	Prefix     string              `json:"prefix"`
	PrefixLen  int32               `json:"prefix_len"`
	LocalIP    string              `json:"router_ip"`
	PeerIP     string              `json:"peer_ip"`
	BaseAttrs  *bgp.BaseAttributes `json:"base_attrs"`
	PeerASN    uint32              `json:"peer_asn"`
	OriginAS   int32               `json:"origin_as"`
	ProtocolID base.ProtoID        `json:"protocol_id"`
	Nexthop    string              `json:"nexthop"`
	Labels     []uint32            `json:"labels"`
	Name       string              `json:"name"`
	PeerName   string              `json:"peer_name"`
}

type bgpNode struct {
	Key             string         `json:"_key,omitempty"`
	ID              string         `json:"_id,omitempty"`
	BGPRouterID     string         `json:"router_id,omitempty"`
	ASN             int32          `json:"asn"`
	AdvCapabilities bgp.Capability `json:"adv_cap,omitempty"`
}
