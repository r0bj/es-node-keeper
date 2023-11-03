package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"gopkg.in/yaml.v2"
)

const (
	ver      string = "0.13"
	interval int    = 30
)

var (
	esUrl         = kingpin.Flag("url", "elasticsearch URL").Default("http://localhost:9200").Short('u').String()
	timeout       = kingpin.Flag("timeout", "timeout for HTTP requests in seconds").Default("10").Short('t').Int()
	config        = kingpin.Flag("config", "config file path").Default("/etc/es-node-keeper.yaml").Short('c').String()
	noRestartTime = kingpin.Flag("no-restart-time", "minimal time in minutes between restarts").Default("10").Short('d').Int()
	dryRun        = kingpin.Flag("dry-run", "dry run").Short('n').Bool()
	verbose       = kingpin.Flag("verbose", "Verbose mode.").Short('v').Bool()
)

type Node struct {
	Name string `json:"name"`
}

type LocalNodes struct {
	Nodes []struct {
		Instance string `yaml:"instance"`
		Service  string `yaml:"service"`
	} `yaml:"nodes"`
}

type ClusterStatus struct {
	Status string `json:"status"`
}

type ClusterSettings struct {
	Transient struct {
		Cluster struct {
			Routing struct {
				Allocation struct {
					Enable string `json:"enable"`
				} `json:"allocation"`
			} `json:"routing"`
		} `json:"cluster"`
	} `json:"transient"`
}

func httpGet(url string) (string, error) {
	client := &http.Client{
		Timeout: time.Second * time.Duration(*timeout),
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	// Close the connection after sending request and reading its response
	// It prevents re-use of TCP connections between requests to the same hosts
	req.Close = true

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func parseNodes(data string) ([]Node, error) {
	var nodes []Node
	err := json.Unmarshal([]byte(data), &nodes)
	if err != nil {
		return nodes, fmt.Errorf("JSON parse failed")
	}

	return nodes, nil
}

func parseClusterStatus(data string) (ClusterStatus, error) {
	var clusterStatus ClusterStatus
	err := json.Unmarshal([]byte(data), &clusterStatus)
	if err != nil {
		return clusterStatus, fmt.Errorf("JSON parse failed")
	}

	return clusterStatus, nil
}

func parseClusterSettings(data string) (ClusterSettings, error) {
	var clusterSettings ClusterSettings
	err := json.Unmarshal([]byte(data), &clusterSettings)
	if err != nil {
		return clusterSettings, fmt.Errorf("JSON parse failed")
	}

	return clusterSettings, nil
}

func parseConfig(file string) (LocalNodes, error) {
	var nodes LocalNodes
	source, err := ioutil.ReadFile(file)
	if err == nil {
		err = yaml.Unmarshal([]byte(source), &nodes)
		if err != nil {
			return nodes, err
		}
	} else {
		slog.Warn("Cannot get local nodes from config file, using empty config", "file", *config)
	}

	return nodes, nil
}

func getActiveNodes(esUrl string) (map[string]struct{}, error) {
	url := esUrl + "/_cat/nodes?h=name&format=json"

	esData, err := httpGet(url)
	if err != nil {
		return map[string]struct{}{}, err
	}

	nodes, err := parseNodes(esData)
	if err != nil {
		return map[string]struct{}{}, err
	}

	result := map[string]struct{}{}
	for _, node := range nodes {
		result[node.Name] = struct{}{}
	}

	return result, nil
}

func getClusterStatus(esUrl string) (string, error) {
	url := esUrl + "/_cluster/health"

	esData, err := httpGet(url)
	if err != nil {
		return "", err
	}

	clusterStatus, err := parseClusterStatus(esData)
	if err != nil {
		return "", err
	}

	return clusterStatus.Status, nil
}

func getClusterRoutingAllocation(esUrl string) (string, error) {
	url := esUrl + "/_cluster/settings"

	esData, err := httpGet(url)
	if err != nil {
		return "", err
	}

	clusterRoutingAllocation, err := parseClusterSettings(esData)
	if err != nil {
		return "", err
	}

	return clusterRoutingAllocation.Transient.Cluster.Routing.Allocation.Enable, nil
}

func getInvalidNodes(localNodes map[string]map[string]interface{}, activeNodes map[string]struct{}) []string {
	var nodesToRestart []string
	for service, value := range localNodes {
		if _, ok := activeNodes[value["instance"].(string)]; !ok {
			nodesToRestart = append(nodesToRestart, service)
		}
	}

	return nodesToRestart
}

func restartSystemdService(service string) error {
	command := "systemctl"
	args := []string{"restart", service}
	cmd := exec.Command(command, args...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(fmt.Sprint(err) + ": " + stderr.String())
	}

	return nil
}

func localNodesToMap(localNodes LocalNodes) map[string]map[string]interface{} {
	nodes := make(map[string]map[string]interface{})
	for _, localNode := range localNodes.Nodes {
		nodes[localNode.Service] = map[string]interface{}{
			"instance":    localNode.Instance,
			"lastRestart": 0,
		}
	}
	return nodes
}

func sleepLoop() {
       time.Sleep(time.Second * time.Duration(interval))
}

func nodeKeeper(esUrl string, localNodes map[string]map[string]interface{}) {
	for {
		activeNodes, err := getActiveNodes(esUrl)
		if err != nil {
			slog.Warn("Cannot get active nodes from cluster")
			sleepLoop()
			continue
		}

		invalidNodes := getInvalidNodes(localNodes, activeNodes)
		if len(invalidNodes) > 0 {
			for _, service := range invalidNodes {
				systemdService := fmt.Sprintf("%s.service", service)

				now := int(time.Now().Unix())
				if now-localNodes[service]["lastRestart"].(int) > *noRestartTime*60 {
					clusterStatus, err := getClusterStatus(esUrl)
					if err != nil {
						slog.Warn("Cannot get cluster status")
						continue
					}

					clusterRoutingAllocation, err := getClusterRoutingAllocation(esUrl)
					if err != nil {
						slog.Warn("Cannot get cluster routing allocation")
						continue
					}

					if clusterRoutingAllocation == "" {
						slog.Warn("Cluster routing allocation is empty")
						continue
					}

					if strings.ToLower(clusterStatus) != "red" && strings.ToLower(clusterRoutingAllocation) == "all" {
						slog.Info("Local node is not an active member of the cluster, restarting service",
							"node",
							localNodes[service]["instance"],
							"service",
							systemdService,
						)
						if *dryRun {
							slog.Info("Dry run, skipping")
						} else {
							if err := restartSystemdService(systemdService); err == nil {
								slog.Info("Service restarted", "service", systemdService)
								localNodes[service]["lastRestart"] = now
							} else {
								slog.Error("Cannot restart service", "service", service, "error", err)
							}
						}
					} else {
						slog.Debug("Cannot restart service due to cluster conditions", "service", service)
					}
				} else {
					slog.Debug("Cannot restart service due to minimal time between restarts", "service", service)
				}
			}
		} else {
			slog.Debug("All local nodes are active members of the cluster")
		}
		sleepLoop()
	}
}

func main() {
	var loggingLevel = new(slog.LevelVar)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: loggingLevel}))
	slog.SetDefault(logger)

	kingpin.Version(ver)
	kingpin.Parse()

	if *verbose {
		loggingLevel.Set(slog.LevelDebug)
	}

	slog.Info("Starting", "version", ver)

	if *dryRun {
		slog.Info("Running in dry run mode")
	}

	localNodes, err := parseConfig(*config)
	if err != nil {
		slog.Error("Cannot get local nodes from config", "file", *config)
		os.Exit(1)
	}

	slog.Info("Loaded", "config", localNodes)
	slog.Info("Elasticsearch URL", "url", *esUrl)

	go nodeKeeper(*esUrl, localNodesToMap(localNodes))
	select {}
}
