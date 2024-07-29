package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"

	"github.com/golang/glog"
	"github.com/jalapeno/ipv4-topology/pkg/arangodb"
	"github.com/jalapeno/ipv4-topology/pkg/kafkamessenger"
	"github.com/jalapeno/ipv4-topology/pkg/kafkanotifier"

	_ "net/http/pprof"
)

const (
	// userFile defines the name of file containing base64 encoded user name
	userFile = "./credentials/.username"
	// passFile defines the name of file containing base64 encoded password
	passFile = "./credentials/.password"
	// MAXUSERNAME defines maximum length of ArangoDB user name
	MAXUSERNAME = 256
	// MAXPASS defines maximum length of ArangoDB password
	MAXPASS = 256
)

var (
	msgSrvAddr          string
	dbSrvAddr           string
	dbName              string
	dbUser              string
	dbPass              string
	lslinkCollection    string
	lsprefixCollection  string
	lsnodeExtCollection string
	bgpNodeCollection   string
	// ebgpSessionCollection  string
	inetPrefixV4Collection string
	lsTopologyV4Collection string
	ipv4TopologyCollection string
)

func init() {
	runtime.GOMAXPROCS(1)
	// flag.StringVar(&msgSrvAddr, "message-server", "198.18.133.104:30092", "URL to the messages supplying server")
	// flag.StringVar(&dbSrvAddr, "database-server", "http://198.18.133.104:30852", "{dns name}:port or X.X.X.X:port of the graph database")
	// flag.StringVar(&dbName, "database-name", "jalapeno", "DB name")
	// flag.StringVar(&dbUser, "database-user", "root", "DB User name")
	// flag.StringVar(&dbPass, "database-pass", "jalapeno", "DB User's password")

	flag.StringVar(&msgSrvAddr, "message-server", "", "URL to the messages supplying server")
	flag.StringVar(&dbSrvAddr, "database-server", "", "{dns name}:port or X.X.X.X:port of the graph database")
	flag.StringVar(&dbName, "database-name", "", "DB name")
	flag.StringVar(&dbUser, "database-user", "", "DB User name")
	flag.StringVar(&dbPass, "database-pass", "", "DB User's password")

	flag.StringVar(&lslinkCollection, "edge-name", "ls_link", "Edge Collection name, default \"ls_link\"")
	flag.StringVar(&lsprefixCollection, "prefix-name", "ls_prefix", "Prefix Collection name, default \"ls_prefix\"")
	flag.StringVar(&lsnodeExtCollection, "lsnodeExtended-name", "ls_node_extended", "ls_node_extended Collection name, default: \"ls_node_extended\"")
	flag.StringVar(&bgpNodeCollection, "bgp-name", "bgp_node", "bgp_node Collection name, default \"bgp_node\"")
	flag.StringVar(&inetPrefixV4Collection, "inetv4-prefix-name", "inet_prefix_v4", "inet prefix v4 Collection name, default \"inet_prefix_v4\"")
	flag.StringVar(&lsTopologyV4Collection, "ls-topology", "ls_topology_v4", "Edge Collection name, default \"ls_topology_v4\"")
	flag.StringVar(&ipv4TopologyCollection, "ipv4-topology", "ipv4_topology", "Edge Collection name, default \"ipv4_topology\"")
}

var (
	onlyOneSignalHandler = make(chan struct{})
	shutdownSignals      = []os.Signal{os.Interrupt}
)

func setupSignalHandler() (stopCh <-chan struct{}) {
	close(onlyOneSignalHandler) // panics when called twice

	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

func main() {
	flag.Parse()
	_ = flag.Set("logtostderr", "true")

	// validateDBCreds check if the user name and the password are provided either as
	// command line parameters or via files. If both are provided command line parameters
	// will be used, if neither, processor will fail.
	if err := validateDBCreds(); err != nil {
		glog.Errorf("failed to validate the database credentials with error: %+v", err)
		os.Exit(1)
	}

	// initialize kafkanotifier to write back processed events into ls_node_edge_events topic
	notifier, err := kafkanotifier.NewKafkaNotifier(msgSrvAddr)
	if err != nil {
		glog.Errorf("failed to initialize events notifier with error: %+v", err)
		os.Exit(1)
	}

	dbSrv, err := arangodb.NewDBSrvClient(dbSrvAddr, dbUser, dbPass, dbName, lslinkCollection,
		lsprefixCollection, lsnodeExtCollection, bgpNodeCollection,
		inetPrefixV4Collection, lsTopologyV4Collection, ipv4TopologyCollection, notifier)
	if err != nil {
		glog.Errorf("failed to initialize database client with error: %+v", err)
		os.Exit(1)
	}

	if err := dbSrv.Start(); err != nil {
		if err != nil {
			glog.Errorf("failed to connect to database with error: %+v", err)
			os.Exit(1)
		}
	}

	// initializing messenger process
	msgSrv, err := kafkamessenger.NewKafkaMessenger(msgSrvAddr, dbSrv.GetInterface())
	if err != nil {
		glog.Errorf("failed to initialize message server with error: %+v", err)
		os.Exit(1)
	}

	msgSrv.Start()

	stopCh := setupSignalHandler()
	<-stopCh

	msgSrv.Stop()
	dbSrv.Stop()

	os.Exit(0)
}

func validateDBCreds() error {
	// Attempting to access username and password files.
	u, err := readAndDecode(userFile, MAXUSERNAME)
	if err != nil {
		if dbUser != "" && dbPass != "" {
			return nil
		}
		return fmt.Errorf("failed to access %s with error: %+v and no username and password provided via command line arguments", userFile, err)
	}
	p, err := readAndDecode(passFile, MAXPASS)
	if err != nil {
		if dbUser != "" && dbPass != "" {
			return nil
		}
		return fmt.Errorf("failed to access %s with error: %+v and no username and password provided via command line arguments", passFile, err)
	}
	dbUser, dbPass = u, p

	return nil
}

func readAndDecode(fn string, max int) (string, error) {
	f, err := os.Open(fn)
	if err != nil {
		return "", err
	}
	defer f.Close()
	l, err := f.Stat()
	if err != nil {
		return "", err
	}
	b := make([]byte, int(l.Size()))
	n, err := io.ReadFull(f, b)
	if err != nil {
		return "", err
	}
	if n > max {
		return "", fmt.Errorf("length of data %d exceeds maximum acceptable length: %d", n, max)
	}
	b = b[:n]

	return string(b), nil
}
