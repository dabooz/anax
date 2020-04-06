package agreementbot

import (
	"errors"
	"github.com/open-horizon/anax/policy"
)

// Implements the policy.IPolicyManager interface

// DO WE NEED THIS ???
// DO WE NEED THIS ???
// DO WE NEED THIS ???
// DO WE NEED THIS ???
// DO WE NEED THIS ???
// DO WE NEED THIS ???

type CombinedPM struct {
	bPolMgr   *BusinessPolicyManager
	patPolMgr *PatternManager
}

func NewAgreementbotPolicyManager(bpm *BusinessPolicyManager, patm *PatternManager) policy.IPolicyManager {
	return &CombinedPM{
		bPolMgr:   bpm,
		patPolMgr: patm,
	}
}

func (c *CombinedPM) NumberPolicies() int {
	return c.bPolMgr.NumberPolicies() + c.patPolMgr.NumberPolicies()
}

func (c *CombinedPM) GetAllAgreementProtocols() map[string]policy.BlockchainList {
	bPolProtocols := c.bPolMgr.GetAllAgreementProtocols()
	patProtocols := c.patPolMgr.GetAllAgreementProtocols()
	for p, bcl := range patProtocols {
		bPolProtocols[p] = bcl
	}
	return bPolProtocols
}

func (c *CombinedPM) UpdatePolicy(org string, newPolicy *policy.Policy) {
	// Not used in the agbot.
}

func (c *CombinedPM) DeletePolicy(org string, delPolicy *policy.Policy) {
	// TODO: fill in the implementation
}

func (c *CombinedPM) GetAllPolicyOrgs() []string {
	// TODO: fill in the implementation - do we need this method?
	// pattern orgs only
	return c.bPolMgr.GetAllPolicyOrgs()
}

func (c *CombinedPM) GetPolicy(org string, name string) *policy.Policy {
	// TODO: fill in the implementation
	return nil
}

func (c *CombinedPM) MatchesMine(org string, matchPolicy *policy.Policy) error {
	// TODO: fill in the implementation
	return nil
}

// This function is used only on the node side. Not implemented in the Agbot.
func (c *CombinedPM) GetPolicyList(homeOrg string, inPolicy *policy.Policy) ([]policy.Policy, error) {
	return nil, errors.New("GetPolicyList nNot implemented in agreement bot.")
}

// This function is used only on the node side. Not implemented in the Agbot.
func (c *CombinedPM) MergeAllProducers(policies *[]policy.Policy, previouslyMerged *policy.Policy) (*policy.Policy, error) {
	return nil, errors.New("MergeAllProducers not implemented in agreement bot.")
}
