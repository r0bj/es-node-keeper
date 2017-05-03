package main

import (
	"fmt"
	"time"
	"strings"
	"encoding/json"
	"io/ioutil"
	"bytes"
	"os/exec"

	"github.com/parnurzeal/gorequest"
	"gopkg.in/alecthomas/kingpin.v2"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

const (
	ver string = "0.10"
	interval int = 30
)

var (
	esURL = kingpin.Flag("url", "elasticsearch URL").Default("http://localhost:9200").Short('u').String()
	timeout = kingpin.Flag("timeout", "timeout for HTTP requests in seconds").Default("10").Short('t').Int()
	config = kingpin.Flag("config", "config file path").Default("/etc/es-node-keeper.yaml").Short('c').String()
	noRestartTime = kingpin.Flag("no-restart-time", "minimal time in minutes between restarts").Default("10").Short('d').Int()
)

// Node : struct contains active node name
type Node struct {
	Name string `json:"name"`
}

// LocalNodes : struct contains local nodes data
type LocalNodes struct {
	Nodes []struct {
		Instance string `yaml:"instance"`
		Service string `yaml:"service"`
	} `yaml:"nodes"`
}

// ClusterStatus : struct contains cluster status data
type ClusterStatus struct {
	Status string `json:"status"`
}

// ClusterSettings : struct contains cluster settings data
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

func esQueryGet(url string, timeout int) (string, error) {
	request := gorequest.New()
	resp, body, errs := request.Get(url).Timeout(time.Duration(timeout) * time.Second).End()

	if errs != nil {
		var errsStr []string
		for _, e := range errs {
			errsStr = append(errsStr, fmt.Sprintf("%s", e))
		}
		return "", fmt.Errorf("%s", strings.Join(errsStr, ", "))
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP response code: %s", resp.Status)
	}
	return body, nil
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
	if err != nil {
		return nodes, err
	}

	err = yaml.Unmarshal([]byte(source), &nodes)
	if err != nil {
		return nodes, err
	}

	return nodes, nil
}

func getActiveNodes(esURL string, timeout int) (map[string]struct{}, error) {
	url := esURL + "/_cat/nodes?h=name&format=json"

	esData, err := esQueryGet(url, timeout)
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

func getClusterStatus(esURL string, timeout int) (string, error) {
	url := esURL + "/_cluster/health"

	esData, err := esQueryGet(url, timeout)
	if err != nil {
		return "", err
	}

	clusterStatus, err := parseClusterStatus(esData)
	if err != nil {
		return "", err
	}

	return clusterStatus.Status, nil
}

func getClusterRoutingAllocation(esURL string, timeout int) (string, error) {
	url := esURL + "/_cluster/settings"

	esData, err := esQueryGet(url, timeout)
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

func restartNode(node string) error {
	command := "service"
	args := []string{node, "restart"}
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
			"instance": localNode.Instance,
			"lastRestart": 0,
		}
	}
	return nodes
}

func nodeKeeper(esURL string, timeout int, localNodes map[string]map[string]interface{}, noRestartTime int) {
	for {
		activeNodes, err := getActiveNodes(esURL, timeout)
		if err != nil {
			log.Warn("Cannot get active nodes from cluster")
			continue
		}

		invalidNodes := getInvalidNodes(localNodes, activeNodes)
		if len(invalidNodes) > 0 {
			for _, service := range invalidNodes {
				now := int(time.Now().Unix())
				if now - localNodes[service]["lastRestart"].(int) > noRestartTime * 60 {
					clusterStatus, err := getClusterStatus(esURL, timeout)
					if err != nil {
						log.Warn("Cannot get cluster status")
						continue
					}

					clusterRoutingAllocation, err := getClusterRoutingAllocation(esURL, timeout)
					if err != nil {
						log.Warn("Cannot get cluster routing allocation")
						continue
					}
								
					if strings.ToLower(clusterStatus) != "red" && strings.ToLower(clusterRoutingAllocation) == "all" {
						log.Infof("Node %s is not active member of cluster, restarting service %s",
							localNodes[service]["instance"],
							service,
						)
						if err := restartNode(service); err == nil {
							log.Infof("Service %s restarted", service)
							localNodes[service]["lastRestart"] = now
						} else {
							log.Errorf("Cannot restart service %s: %s", service, err)
						}
					} else {
						log.Debugf("Cannot restart service %s due to cluster conditions", service)
					}
				} else {
					log.Debugf("Cannot restart service %s due to minimal time between restarts parameter", service)
				}
			}
		}
		time.Sleep(time.Second * time.Duration(interval))
	}
}

func main() {
	kingpin.Version(ver)
	kingpin.Parse()

	localNodes, err := parseConfig(*config)
	if err != nil {
		log.Fatalf("Cannot get local nodes from config file %s", *config)
	}

	go nodeKeeper(*esURL, *timeout, localNodesToMap(localNodes), *noRestartTime)
	select {}
}
