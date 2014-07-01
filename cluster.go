package esmc

import (
	es "github.com/maxjakob/elasticsearch"
)

type Cluster struct {
	Name    string
	Mode    Mode
	cluster *es.Cluster
}

// Connects to a cluster using the provided Config.
func NewCluster(config Config) *Cluster {
	return &Cluster{
		Name: config.Name(),
		Mode: config.Mode(),
		cluster: es.NewCluster(
			config.Endpoints,
			config.PingInterval(),
			config.PingTimeout(),
		),
	}
}

// Wraps es.Cluster.Shutdown
func (c *Cluster) Shutdown() {
	c.cluster.Shutdown()
}

func (c *Cluster) labels(requestType string, ok bool) map[string]string {
	labels := map[string]string{
		"cluster":      c.Name,
		"cluster_mode": c.Mode.String(),
		"request_type": requestType,
	}

	if ok {
		labels["outcome"] = "success"
	} else {
		labels["outcome"] = "failed"
	}

	return labels
}

func (c *Cluster) Execute(f es.Fireable, response interface{}) (err error) {
	return c.cluster.Execute(f, response)
}

func (c *Cluster) Index(r es.IndexRequest) (_ es.IndexResponse, err error) {
	return c.cluster.Index(r)
}

func (c *Cluster) Update(r es.UpdateRequest) (_ es.IndexResponse, err error) {
	return c.cluster.Update(r)
}

func (c *Cluster) Delete(r es.DeleteRequest) (_ es.IndexResponse, err error) {
	return c.cluster.Delete(r)
}

func (c *Cluster) Create(r es.CreateRequest) (_ es.IndexResponse, err error) {
	return c.cluster.Create(r)
}

func (c *Cluster) Bulk(r es.BulkRequest) (_ es.BulkResponse, err error) {
	return c.cluster.Bulk(r)
}

func (c *Cluster) MultiSearch(r es.MultiSearchRequest) (resp es.MultiSearchResponse, err error) {
	return c.cluster.MultiSearch(r)
}

func (c *Cluster) Search(r es.SearchRequest) (response es.SearchResponse, err error) {
	return c.cluster.Search(r)
}
