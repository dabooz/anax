package matchcache

import (
	"fmt"
	//"github.com/golang/glog"
	"github.com/open-horizon/anax/cutil"
	"github.com/open-horizon/anax/externalpolicy"
	"github.com/open-horizon/anax/policy"
	"sync"
)

// The Node policy to deployment policy match cache, used to speed up matching nodes with deployment policy. The assumption
// in the design is there will be many nodes with the same node policy, and therefore when new nodes come along it will be easy
// to find the matching deployment policies and make agreements quickly.

type MatchCache struct {
	mapLock               sync.Mutex                                   // Lock that controls concurrent access to all maps in the MatchCache.
	KnownNodeMatches      map[NodePolicyHashString]DeploymentPolicyMap // Hashed node policy mapped to a set of deployment policies.
	NodeIdToPolicyHashMap map[string]NodePolicyHashString              // A map of all known nodes (node id) indicating the node policy hash for that node's policy.
	NodePolicies          map[NodePolicyHashString]*policy.Policy      // A map of all known node policy hashes an the node policy they were derived from.
}

// Create a new MatchCache
func NewMatchCache() *MatchCache {
	// Instances of DeploymentPolicyMap have to be created as needed when node policy hashes are added.
	return &MatchCache{
		KnownNodeMatches:      make(map[NodePolicyHashString]DeploymentPolicyMap),
		NodeIdToPolicyHashMap: make(map[string]NodePolicyHashString),
		NodePolicies:          make(map[NodePolicyHashString]*policy.Policy),
	}
}

// Add a new node and deployment policy association to the cache.
func (m *MatchCache) CacheNodeAndDeploymentPolicy(nodeId string, np *externalpolicy.ExternalPolicy, depPolId string) error {

	m.mapLock.Lock()
	defer m.mapLock.Unlock()

	npHash, err := HashNodePolicy(np)
	if err != nil {
		return err
	}

	if _, ok := m.NodeIdToPolicyHashMap[nodeId]; !ok {
		if err := m.cacheNodePolicy(nodeId, npHash, np); err != nil {
			return err
		}
	}

	if _, ok := m.KnownNodeMatches[npHash]; !ok {
		m.KnownNodeMatches[npHash] = make(DeploymentPolicyMap)
	}

	m.KnownNodeMatches[npHash][DeploymentPolicyId(depPolId)] = true

	return nil

}

func (m *MatchCache) cacheNodePolicy(nodeId string, npHash NodePolicyHashString, np *externalpolicy.ExternalPolicy) error {
	if _, ok := m.NodePolicies[npHash]; !ok {
		nodePolicy, err := policy.GenPolicyFromExternalPolicy(np, nodeId)
		if err != nil {
			return err
		}
		m.NodePolicies[npHash] = nodePolicy
	}
	m.NodeIdToPolicyHashMap[nodeId] = npHash
	return nil
}

// Update the node policy in the cache. The updated node policy might already exist in the cache if some other
// node is using the same policy. Further, if the cache is found to be missing pieces, then do some
// cleanup to try to correct all the maps.
func (m *MatchCache) UpdateNodePolicy(nodeId string, np *externalpolicy.ExternalPolicy) (bool, error) {

	m.mapLock.Lock()
	defer m.mapLock.Unlock()

	// Make a hash for the new policy.
	npHash, err := HashNodePolicy(np)
	if err != nil {
		return false, err
	}

	// Get the current hash for the node. If there is no node policy cached yet, then cache it and return.
	oldHash, ok := m.NodeIdToPolicyHashMap[nodeId]
	if !ok {
		m.cacheNodePolicy(nodeId, npHash, np)
		return true, nil
	}

	// If nothing has changed, then there is nothing to do.
	if oldHash == npHash {
		return false, nil
	}

	// Get the current set of Deployment policies that are compatible with this node policy. If there
	// are none, then remove the old nodeId to hash mapping and replace it with the new one.
	oldDPM, ok := m.KnownNodeMatches[oldHash]
	if !ok {
		// Leave the NodePolicies[oldhash] map unchanged. That policy might come back again in the future.
		m.cacheNodePolicy(nodeId, npHash, np)
		return true, nil
	}

	// Remember, many nodes might be using the same policy so its possible that deployment
	// policies have already been associated with this new node policy hash. If so, then just
	// update the node policy itself.
	if _, ok := m.KnownNodeMatches[npHash]; ok {
		m.cacheNodePolicy(nodeId, npHash, np)
		return true, nil
	}

	// The new node policy hash should be associated with the deployment policies of the old policy
	// because we don't yet know which deployment policies are compatible. This code assumes that
	// the caller will perform these checks and invalidate the cache if necessary. If another caller
	// were to find one of these cache entries before it could lbe invalidated, that should be ok. It
	// is the caller's responsibility to do the compatibility and not blindly trust the cache.

	// There are no known matches with this new hash.
	m.KnownNodeMatches[npHash] = make(DeploymentPolicyMap)
	for depPolId, _ := range oldDPM {
		m.KnownNodeMatches[npHash][DeploymentPolicyId(depPolId)] = true
	}

	return true, nil

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

// The input node need to be removed from the cache.
func (m *MatchCache) InvalidateNode(nodeId string) {

	m.mapLock.Lock()
	defer m.mapLock.Unlock()

	npHash, ok := m.NodeIdToPolicyHashMap[nodeId]
	if !ok {
		return
	}
	delete(m.NodeIdToPolicyHashMap, nodeId)

	// If there are no more nodes using this hash, then clean up the maps which index
	// by node policy hash.
	found := false
	for _, cachedNPHash := range m.NodeIdToPolicyHashMap {
		if cachedNPHash == npHash {
			found = true
			break
		}
	}

	if !found {
		delete(m.KnownNodeMatches, npHash)
		delete(m.NodePolicies, npHash)
	}

}

// The combination of the node policy and deployment policy has been found to be incompatible,
// so dis-associate them in the cache (if they are associated).
func (m *MatchCache) InvalidateNodeWithPolicy(nodeId string, depPolId string) {

	m.mapLock.Lock()
	defer m.mapLock.Unlock()

	npHash, ok := m.NodeIdToPolicyHashMap[nodeId]
	if !ok {
		return
	}

	// If there are no matches with this hash, just return.
	if _, ok := m.KnownNodeMatches[npHash]; !ok {
		return
	}

	// Remove the association between the node policy and the deployment policy.
	if _, ok := m.KnownNodeMatches[npHash][DeploymentPolicyId(depPolId)]; ok {
		delete(m.KnownNodeMatches[npHash], DeploymentPolicyId(depPolId))
	}

}

// The input deployment policy has changed. Invalidate the cache entries related to this change. Also
// return the list of nodes that need to be re-evaluated based on this change.
func (m *MatchCache) InvalidateDeploymentPolicy(depPolId string) []string {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()

	// For every node policy hash, check to see which ones refer to the updated policy. Keep a list of matching
	// node policy hashes so that a list of affected nodes can be returned.
	npHashList := make([]string, 0, 10)
	for npHash, dpm := range m.KnownNodeMatches {
		if _, ok := dpm[DeploymentPolicyId(depPolId)]; ok {
			// Assume that this node policy is no longer compatible with the updated deployment policy.
			delete(m.KnownNodeMatches[npHash], DeploymentPolicyId(depPolId))

			// If there are no more deployment policies associated with this node policy hash, then delete the
			// hash from the map too.
			if len(m.KnownNodeMatches[npHash]) == 0 {
				delete(m.KnownNodeMatches, npHash)
			}

			// Remember the node policy hash that is affected by this change.
			npHashList = append(npHashList, string(npHash))
		}
	}

	// For every node policy hash affected by the deployment policy change, find all the nodes with that
	// same node policy hash and return them.
	nodeList := make([]string, 0, 10)
	for nodeId, npHash := range m.NodeIdToPolicyHashMap {
		if cutil.SliceContains(npHashList, string(npHash)) {
			nodeList = append(nodeList, nodeId)
		}
	}

	return nodeList
}

// Return the list of node policies that are known to be compatible with the input deployment
// policy.
func (m *MatchCache) GetCompatibleNodePolicies(depPolId string) []*policy.Policy {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()

	nps := make([]*policy.Policy, 0, 10)

	// Iterate each node policy hash, looking for the input deployment policy. 
	for npHash, dpm := range m.KnownNodeMatches {
		if _, ok := dpm[DeploymentPolicyId(depPolId)]; ok {
			// Found the deployment policy, make a safe copy of the node policy and
			// save it in the return array.
			polCopy := m.NodePolicies[npHash].DeepCopy()
			nps = append(nps, polCopy)
		}
	}

	return nps
}

// Dump the contents to the match cache to a string so it can be cached.
func (m *MatchCache) String() string {
	m.mapLock.Lock()
	defer m.mapLock.Unlock()
	return fmt.Sprintf("Known node matches: %v, Cached nodes %v, Node Policies %v", m.KnownNodeMatches, m.NodeIdToPolicyHashMap, m.NodePolicies)
}
