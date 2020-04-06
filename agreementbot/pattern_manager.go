package agreementbot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/exchange"
	"github.com/open-horizon/anax/policy"
	"golang.org/x/crypto/sha3"
	"sync"
	"time"
)

type PatternEntry struct {
	Pattern  *exchange.Pattern // The pattern definition from the exchange.
	Updated  uint64            // The (epoch) time when this entry was updated.
	Hash     []byte            // A hash of the current entry to compare for changes in the future.
	Policies []*policy.Policy  // The list of policies generated for this pattern.
}

func (p *PatternEntry) String() string {
	return fmt.Sprintf("Pattern Entry: "+
		"Updated: %v "+
		"Hash: %x "+
		"Policies: %v"+
		"Pattern: %v",
		p.Updated, p.Hash, p.Policies, p.Pattern)
}

func (p *PatternEntry) ShortString() string {
	return fmt.Sprintf("Policies: %v", p.Policies)
}

func hashPattern(p *exchange.Pattern) ([]byte, error) {
	if ps, err := json.Marshal(p); err != nil {
		return nil, errors.New(fmt.Sprintf("unable to marshal pattern %v to a string, error %v", p, err))
	} else {
		hash := sha3.Sum256([]byte(ps))
		return hash[:], nil
	}
}

func NewPatternEntry(p *exchange.Pattern) (*PatternEntry, error) {
	pe := new(PatternEntry)
	pe.Pattern = p
	pe.Updated = uint64(time.Now().Unix())
	if hash, err := hashPattern(p); err != nil {
		return nil, err
	} else {
		pe.Hash = hash
	}
	pe.Policies = make([]*policy.Policy, 0, 10)
	return pe, nil
}

func (pe *PatternEntry) AddPolicy(p *policy.Policy) {
	pe.Policies = append(pe.Policies, p)
}

func (pe *PatternEntry) DeleteAllPolicies() {
	pe.Policies = make([]*policy.Policy, 0, 10)
}

func (pe *PatternEntry) UpdateEntry(pattern *exchange.Pattern, newHash []byte) {
	pe.Pattern = pattern
	pe.Hash = newHash
	pe.Updated = uint64(time.Now().Unix())
	pe.Policies = make([]*policy.Policy, 0, 10)
}

type PatternManager struct {
	spMapLock      sync.Mutex                          // The lock that protects the map of ServedPatterns.
	ServedPatterns map[string]exchange.ServedPattern   // The served node org, pattern org and pattern triplets.
	patMapLock     sync.Mutex                          // The lock that protects the map of patterns.
	OrgPatterns    map[string]map[string]*PatternEntry // All patterns served by this agbot.
}

func (pm *PatternManager) String() string {
	pm.patMapLock.Lock()
	defer pm.patMapLock.Unlock()

	res := "Pattern Manager: "
	for org, orgMap := range pm.OrgPatterns {
		res += fmt.Sprintf("Org: %v ", org)
		for pat, pe := range orgMap {
			res += fmt.Sprintf("Pattern: %v %v\n", pat, pe)
		}
	}

	pm.spMapLock.Lock()
	defer pm.spMapLock.Unlock()

	for _, served := range pm.ServedPatterns {
		res += fmt.Sprintf(" Serve: %v ", served)
	}
	return res
}

func (pm *PatternManager) ShortString() string {

	pm.patMapLock.Lock()
	defer pm.patMapLock.Unlock()

	res := "Pattern Manager: "
	for org, orgMap := range pm.OrgPatterns {
		res += fmt.Sprintf("Org: %v ", org)
		for pat, pe := range orgMap {
			s := ""
			if pe != nil {
				s = pe.ShortString()
			}
			res += fmt.Sprintf("Pattern: %v %v ", pat, s)
		}
	}
	return res
}

func NewPatternManager() *PatternManager {
	pm := &PatternManager{
		OrgPatterns: make(map[string]map[string]*PatternEntry),
	}
	return pm
}

func (pm *PatternManager) hasOrg(org string) bool {
	_, ok := pm.OrgPatterns[org]
	return ok
}

func (pm *PatternManager) hasPattern(org string, pattern string) bool {
	if pm.hasOrg(org) {
		_, ok := pm.OrgPatterns[org][pattern]
		return ok
	}
	return false
}

// copy the given map of served patterns
func (pm *PatternManager) setServedPatterns(servedPatterns map[string]exchange.ServedPattern) {
	pm.spMapLock.Lock()
	defer pm.spMapLock.Unlock()

	// copy the input map
	// TODO: Not safe
	pm.ServedPatterns = servedPatterns
}

// check if the agbot serves the given pattern or not.
func (pm *PatternManager) servePattern(pattern_org string, pattern string) bool {
	pm.spMapLock.Lock()
	defer pm.spMapLock.Unlock()

	for _, sp := range pm.ServedPatterns {
		if sp.PatternOrg == pattern_org && (sp.Pattern == pattern || sp.Pattern == "*") {
			return true
		}
	}
	return false
}

// check if the agbot serves the given org or not.
func (pm *PatternManager) serveOrg(pattern_org string) bool {
	pm.spMapLock.Lock()
	defer pm.spMapLock.Unlock()

	for _, sp := range pm.ServedPatterns {
		if sp.PatternOrg == pattern_org {
			return true
		}
	}
	return false
}

// return an array of node orgs for the given served pattern org and pattern.
// this function is called from a different thread.
func (pm *PatternManager) GetServedNodeOrgs(pattten_org string, pattern string) []string {
	pm.spMapLock.Lock()
	defer pm.spMapLock.Unlock()

	node_orgs := []string{}
	for _, sp := range pm.ServedPatterns {
		if sp.PatternOrg == pattten_org && (sp.Pattern == pattern || sp.Pattern == "*") {
			node_org := sp.NodeOrg
			// the default node org is the pattern org
			if node_org == "" {
				node_org = sp.PatternOrg
			}
			node_orgs = append(node_orgs, node_org)
		}
	}
	return node_orgs
}

// Given a list of pattern_org/pattern/node_org triplets that this agbot is supposed to serve, save that list and
// convert it to map of maps (keyed by org and pattern name) to hold the pattern definition. This
// will allow the PatternManager to know when the pattern metadata changes.
func (pm *PatternManager) SetCurrentPatterns(servedPatterns map[string]exchange.ServedPattern) error {

	pm.patMapLock.Lock()
	defer pm.patMapLock.Unlock()

	// Exit early if nothing to do
	if len(pm.OrgPatterns) == 0 && len(servedPatterns) == 0 {
		return nil
	}

	// save the served patterns in the pm
	pm.setServedPatterns(servedPatterns)

	// If this is the first time to set the served patterns, create a new map of maps.
	if len(pm.OrgPatterns) == 0 {
		pm.OrgPatterns = make(map[string]map[string]*PatternEntry)
	}

	// For each org that this agbot is supposed to be serving, check if it is already in the pm.
	// If not add to it. The patterns will be added later in the UpdatePatternPolicies function.
	for _, served := range servedPatterns {
		// If we have encountered a new org in the served pattern list, create a map of patterns for it.
		if !pm.hasOrg(served.PatternOrg) {
			pm.OrgPatterns[served.PatternOrg] = make(map[string]*PatternEntry)
		}
	}

	// For each org in the existing PatternManager, check to see if its in the new map. If not, then
	// this agbot is no longer serving any patterns in that org, we can get rid of everything in that org.
	for org, _ := range pm.OrgPatterns {
		if !pm.serveOrg(org) {
			// Delete org and all policies in it.
			glog.V(5).Infof("Deleting the org %v from the pattern manager and all its policies because it is no longer hosted by the agbot.", org)
			if err := pm.deleteOrg(org); err != nil {
				return err
			}
		}
	}

	return nil
}

// Create all the policies for the input pattern.
func createPolicies(pe *PatternEntry, patternId string, pattern *exchange.Pattern) error {
	var err error
	if pe.Policies, err = exchange.ConvertToPolicies(patternId, pattern); err != nil {
		return errors.New(fmt.Sprintf("error converting pattern to policies, error %v", err))
		// } else {
		// for _, pol := range policies {
		// if fileName, err := policy.CreatePolicyFile(policyPath, org, pol.Header.Name, pol); err != nil {
		// 	return errors.New(fmt.Sprintf("error creating policy file, error %v", err))
		// } else {
		// pe.Policies = policies
		// }
		// }
	}
	return nil
}

// For each org that the agbot is supporting, take the set of patterns defined within the org and save them into
// the PatternManager. When new or updated patterns are discovered, generate policies for each pattern so that
// the agbot can start serving the workloads and services.
func (pm *PatternManager) UpdatePatternPolicies(org string, definedPatterns map[string]exchange.Pattern) error {

	pm.patMapLock.Lock()
	defer pm.patMapLock.Unlock()

	// Exit early on error
	if !pm.hasOrg(org) {
		return errors.New(fmt.Sprintf("org %v not found in pattern manager", org))
	}

	// If there is no pattern in the org, delete the org from the pm and all of the policies in the org.
	// This is the case where pattern or the org has been deleted but the agbot still hosts the pattern on the exchange.
	if definedPatterns == nil || len(definedPatterns) == 0 {
		// Delete org and all policies in it.
		glog.V(5).Infof("Deleting the org %v from the pattern manager and all its policies because it does not contain a pattern.", org)
		return pm.deleteOrg(org)
	}

	// Delete the pattern from the pm and all of its policies if the pattern does not exist on the exchange or the agbot
	// does not serve it any more.
	for pattern, _ := range pm.OrgPatterns[org] {
		need_delete := true
		if pm.servePattern(org, pattern) {
			for patternId, _ := range definedPatterns {
				if exchange.GetId(patternId) == pattern {
					need_delete = false
					break
				}
			}
		}

		if need_delete {
			glog.V(5).Infof("Deleting pattern %v and its policies from the org %v because the pattern no longer exists.", pattern, org)
			if err := pm.deletePattern(org, pattern); err != nil {
				return err
			}
		}
	}

	// Now we just need to handle adding new patterns or update existing patterns
	for patternId, pattern := range definedPatterns {
		if !pm.servePattern(org, exchange.GetId(patternId)) {
			continue
		}

		need_new_entry := true
		if pm.hasPattern(org, exchange.GetId(patternId)) {
			if pe := pm.OrgPatterns[org][exchange.GetId(patternId)]; pe != nil {
				need_new_entry = false

				// The PatternEntry is already there, so check if the pattern definition has changed.
				newHash, err := hashPattern(&pattern)
				if err != nil {
					return errors.New(fmt.Sprintf("unable to hash pattern %v for %v, error %v", pattern, org, err))
				}
				if !bytes.Equal(pe.Hash, newHash) {
					glog.V(5).Infof("Deleting all the policies for org %v because the old pattern %v does not match the new pattern %v", org, pe.Pattern, pattern)
					pe.DeleteAllPolicies()
					pe.UpdateEntry(&pattern, newHash)
					glog.V(5).Infof("Creating the policies for pattern %v.", patternId)
					if err := createPolicies(pe, patternId, &pattern); err != nil {
						return errors.New(fmt.Sprintf("unable to create policies for %v, error %v", pattern, err))
					}
				}
			}
		}

		//If there's no PatternEntry yet, create one and then create the policies.
		if need_new_entry {
			if newPE, err := NewPatternEntry(&pattern); err != nil {
				return errors.New(fmt.Sprintf("unable to create pattern entry for %v, error %v", pattern, err))
			} else {
				pm.OrgPatterns[org][exchange.GetId(patternId)] = newPE
				glog.V(5).Infof("Creating the policies for pattern %v.", patternId)
				if err := createPolicies(newPE, patternId, &pattern); err != nil {
					return errors.New(fmt.Sprintf("unable to create policies for %v, error %v", pattern, err))
				}
			}
		}
	}

	return nil
}

// When an org is removed from the list of supported orgs and patterns, remove the org
// from the PatternManager.
func (pm *PatternManager) deleteOrg(org string) error {

	// // Delete all the policy files that are pattern based for the org
	// if err := policy.DeletePolicyFilesForOrg(policyPath, org, true); err != nil {
	// 	glog.Errorf("Error deleting policy files for org %v. %v", org, err)
	// }

	// Get rid of the org map
	if pm.hasOrg(org) {
		delete(pm.OrgPatterns, org)
	}

	return nil
}

// When a pattern is removed, remove the pattern from the PatternManager.
func (pm *PatternManager) deletePattern(org string, pattern string) error {

	// // delete the policy files
	// if err := policy.DeletePolicyFilesForPattern(org, pattern); err != nil {
	// 	glog.Errorf("Error deleting policy files for pattern %v/%v. %v", org, pattern, err)
	// }

	// Get rid of the pattern from the pm
	if pm.hasOrg(org) {
		if _, ok := pm.OrgPatterns[org][pattern]; ok {
			delete(pm.OrgPatterns[org], pattern)
		}
	}

	return nil
}

func (pm *PatternManager) NumberPolicies() int {
	return 0
}

func (pm *PatternManager) GetAllAgreementProtocols() map[string]policy.BlockchainList {
	return map[string]policy.BlockchainList{}
}

// Returns a safe array of orgs served by this agbot.
func (pm *PatternManager) GetAllPatternOrgs() []string {

	pm.patMapLock.Lock()
	defer pm.patMapLock.Unlock()

	orgs := make([]string, 0, len(pm.OrgPatterns))
	for org, _ := range pm.OrgPatterns {
		orgs = append(orgs, org)
	}
	return orgs
}

// Returns a safe array of patterns served by this agbot.
func (pm *PatternManager) GetAllPatterns(org string) []string {

	pm.patMapLock.Lock()
	defer pm.patMapLock.Unlock()

	var orgs []string

	if pm.hasOrg(org) {
		orgs = make([]string, 0, len(pm.OrgPatterns[org]))
		for org, _ := range pm.OrgPatterns[org] {
			orgs = append(orgs, org)
		}
	}
	return orgs
}

// Returns a copy of the generated policies.
func (pm *PatternManager) GetPatternPolicies(org string, patternName string) []*policy.Policy {

	pm.patMapLock.Lock()
	defer pm.patMapLock.Unlock()

	var policies []*policy.Policy

	if pm.hasPattern(org, patternName) {
		pe := pm.OrgPatterns[org][patternName]
		for _, pol := range pe.Policies {
			polCopy := pol.DeepCopy()
			policies = append(policies, polCopy)
		}
	}
	return policies
}
