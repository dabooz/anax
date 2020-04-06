package matchcache

import (
	"fmt"
	"github.com/open-horizon/anax/externalpolicy"
	"sync"
)

// The Node policy to deployment policy match cache, used to speed up matching nodes with deployment policy.
type MatchCache struct {
	mapLock          sync.Mutex // Lock that controls concurrent access to the MatchCache
	KnownNodeMatches map[NodePolicyHashString]DeploymentPolicyMap // Hashed node policy mapped to a set of deployment policies
}

// Create a new MatchCache
func NewMatchCache() *MatchCache {
	// Instances of DeploymentPolicyMap have to be created as needed when node policy hashes are added.
	return &MatchCache{
		KnownNodeMatches: make(map[NodePolicyHashString]DeploymentPolicyMap),
	}
}

// Add a new node entry to the cache.
func (m *MatchCache) CacheNode(np *externalpolicy.ExternalPolicy, bPolId string) error {

	m.mapLock.Lock()
	defer m.mapLock.Unlock()

	npHash, err := HashNodePolicy(np)
	if err != nil {
		return err
	}

	if _, ok := m.KnownNodeMatches[npHash]; !ok {
		m.KnownNodeMatches[npHash] = make(DeploymentPolicyMap)
	}

	m.KnownNodeMatches[npHash][DeploymentPolicyId(bPolId)] = true

	return nil

}

// Get the deployment policies that are already known to match the input node policy.
func (m *MatchCache) GetCachedPolicies(np *externalpolicy.ExternalPolicy) (DeploymentPolicyMap, error) {

	m.mapLock.Lock()
	defer m.mapLock.Unlock()

	npHash, err := HashNodePolicy(np)
	if err != nil {
		return nil, err
	}

	if dpm, ok := m.KnownNodeMatches[npHash]; !ok {
		return nil, nil
	} else {
		return dpm, nil
	}

}


// Dump the contents to the match cache to a string so it can be cached.
func (m MatchCache) String() string {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	return fmt.Sprintf("%v", m.KnownNodeMatches)
}