// +build unit

package matchcache

import (
	"flag"
	"github.com/open-horizon/anax/policy"
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

	// Add a policy directly
	fakeBPolID := BusinessPolicyId("myorg/bpol1")
	fakePol := policy.Policy_Factory("Fake Policy")
	fakeNPolHash := NodePolicyHashString("1234567890abcdefg")

	mc.KnownNodeMatches[fakeNPolHash] = make(MergedDeploymentPolicyMap)
	mc.KnownNodeMatches[fakeNPolHash][fakeBPolID] = fakePol

	// Test and retrieve the fake policy
	if len(mc.KnownNodeMatches) != 1 {
		t.Errorf("Error MatchCache should have 1 entry, is %v", mc)
	} else if bpolMap, ok := mc.KnownNodeMatches[fakeNPolHash]; !ok {
		t.Errorf("Error MatchCache should have entry for %v", fakeNPolHash)
	} else if len(bpolMap) != 1 {
		t.Errorf("Error MatchCache bpol map should have 1 entry, is %v", bpolMap)
	} else if pol, ok := bpolMap[fakeBPolID]; !ok {
		t.Errorf("Error MatchCache should have entry for %v", fakeBPolID)
	} else if pol != fakePol {
		t.Errorf("Error retrieved pol is different from inserted pol")
	}

}