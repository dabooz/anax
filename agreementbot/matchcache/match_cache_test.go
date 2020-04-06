// +build unit

package matchcache

import (
	"flag"
	"testing"
)

func init() {
	flag.Set("alsologtostderr", "true")
	flag.Set("v", "7")
	// no need to parse flags, that's done by test framework
}

// Test direct manipulation of the match cache fields.
func Test_match_cache_simple0(t *testing.T) {

	// Create a new Match Cache
	mc := NewMatchCache()

	if mc == nil {
		t.Errorf("Error creating new MatchCache")
	}

	// Add a deployment policy.
	fakeDPolID := DeploymentPolicyId("myorg/dpol1")
	fakeNPolHash := NodePolicyHashString("1234567890abcdefg")

	mc.KnownNodeMatches[fakeNPolHash] = make(DeploymentPolicyMap)
	mc.KnownNodeMatches[fakeNPolHash][fakeDPolID] = true

	// Test and retrieve from the cache
	if len(mc.KnownNodeMatches) != 1 {
		t.Errorf("Error MatchCache should have 1 entry, is %v", mc)
	} else if dpolMap, ok := mc.KnownNodeMatches[fakeNPolHash]; !ok {
		t.Errorf("Error MatchCache should have entry for %v", fakeNPolHash)
	} else if len(dpolMap) != 1 {
		t.Errorf("Error MatchCache bpol map should have 1 entry, is %v", dpolMap)
	} else if _, ok := dpolMap[fakeDPolID]; !ok {
		t.Errorf("Error MatchCache should have entry for %v", fakeDPolID)
	}

}
