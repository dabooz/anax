package agreementbot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/businesspolicy"
	"github.com/open-horizon/anax/cutil"
	"github.com/open-horizon/anax/events"
	"github.com/open-horizon/anax/exchange"
	"github.com/open-horizon/anax/externalpolicy"
	"github.com/open-horizon/anax/policy"
	"reflect"
	"sync"
	"time"
)

type ServicePolicyEntry struct {
	Policy  *externalpolicy.ExternalPolicy `json:"policy,omitempty"`      // the metadata for this service policy from the exchange.
	Updated uint64                         `json:"updatedTime,omitempty"` // the time when this entry was updated
	Hash    []byte                         `json:"hash,omitempty"`        // a hash of the service policy to compare for metadata changes in the exchange
}

func (p *ServicePolicyEntry) String() string {
	return fmt.Sprintf("ServicePolicyEntry: "+
		"Updated: %v "+
		"Hash: %x "+
		"Policy: %v",
		p.Updated, p.Hash, p.Policy)
}

func (p *ServicePolicyEntry) ShortString() string {
	return p.String()
}

func NewServicePolicyEntry(p *externalpolicy.ExternalPolicy, svcId string) (*ServicePolicyEntry, error) {
	pSE := new(ServicePolicyEntry)
	pSE.Updated = uint64(time.Now().Unix())
	if hash, err := cutil.HashObject(p); err != nil {
		return nil, err
	} else {
		pSE.Hash = hash
	}
	pSE.Policy = p

	return pSE, nil
}

type BusinessPolicyEntry struct {
	Policy          *policy.Policy                 `json:"policy,omitempty"`          // the metadata for this business policy from the exchange, it is converted to the internal policy format
	Updated         uint64                         `json:"updatedTime,omitempty"`     // the time when this entry was updated
	Hash            []byte                         `json:"hash,omitempty"`            // a hash of the business policy to compare for metadata changes in the exchange
	ServicePolicies map[string]*ServicePolicyEntry `json:"servicePolicies,omitempty"` // map of the service id and service policies
}

// Create a new BusinessPolicyEntry. It converts the businesspolicy to internal policy format.
// The business policy exchange id (org/id) is the header name for the internal generated policy.
func NewBusinessPolicyEntry(pol *businesspolicy.BusinessPolicy, polId string) (*BusinessPolicyEntry, error) {
	pBE := new(BusinessPolicyEntry)
	pBE.Updated = uint64(time.Now().Unix())
	if hash, err := cutil.HashObject(pol); err != nil {
		return nil, err
	} else {
		pBE.Hash = hash
	}
	pBE.ServicePolicies = make(map[string]*ServicePolicyEntry, 0)

	// Validate and convert the exchange business policy to internal policy format.
	if err := pol.Validate(); err != nil {
		return nil, fmt.Errorf("Failed to validate the deployment policy %v. %v", *pol, err)
	} else if pPolicy, err := pol.GenPolicyFromBusinessPolicy(polId); err != nil {
		return nil, fmt.Errorf("Failed to convert the deployment policy to internal policy format: %v. %v", *pol, err)
	} else {
		pBE.Policy = pPolicy
	}

	return pBE, nil
}

func (p *BusinessPolicyEntry) String() string {
	return fmt.Sprintf("DeploymentPolicyEntry: "+
		"Updated: %v "+
		"Hash: %x "+
		"Policy: %v"+
		"ServicePolicies: %v",
		p.Updated, p.Hash, p.Policy, p.ServicePolicies)
}

func (p *BusinessPolicyEntry) ShortString() string {
	keys := make([]string, 0, len(p.ServicePolicies))
	for k, _ := range p.ServicePolicies {
		keys = append(keys, k)
	}
	return fmt.Sprintf("DeploymentPolicyEntry: "+
		"Updated: %v "+
		"Hash: %x "+
		"Policy: %v"+
		"ServicePolicies: %v",
		p.Updated, p.Hash, p.Policy.Header.Name, keys)
}

// Add a service policy to a BusinessPolicyEntry.
// Returns true if there is an existing entry for the service id, and it is updated with the new policy.
// If the old and new service policies are same, it returns false.
func (p *BusinessPolicyEntry) AddServicePolicy(svcPolicy *externalpolicy.ExternalPolicy, svcId string) (bool, error) {
	if svcPolicy == nil || svcId == "" {
		return false, nil
	}

	pSE, err := NewServicePolicyEntry(svcPolicy, svcId)
	if err != nil {
		return false, err
	}

	servicePol, found := p.ServicePolicies[svcId]
	if !found {
		p.ServicePolicies[svcId] = pSE
		return false, nil
	} else {
		if !bytes.Equal(pSE.Hash, servicePol.Hash) {
			p.ServicePolicies[svcId] = pSE
			p.Updated = uint64(time.Now().Unix())
			return true, nil
		} else {
			// same service policy exists, do nothing
			return false, nil
		}
	}
}

// Remove a service policy from a BusinessPolicyEntry.
// It returns true if the service policy exists and is removed.
func (p *BusinessPolicyEntry) RemoveServicePolicy(svcId string) bool {
	if svcId == "" {
		return false
	}

	spe, found := p.ServicePolicies[svcId]
	if !found {
		return false
	} else {
		// An empty policy is also tracked in the business policy manager, this way we know if there is
		// new service policy added later.
		// The business policy manager does not track all the service policies referenced by a business policy.
		// It only tracks the ones that have agreements associated with it.
		tempPol := new(externalpolicy.ExternalPolicy)
		if !reflect.DeepEqual(*tempPol, *spe.Policy) {
			delete(p.ServicePolicies, svcId)

			// update the timestamp
			p.Updated = uint64(time.Now().Unix())
			return true
		} else {
			return false
		}
	}
}

func (pe *BusinessPolicyEntry) DeleteAllServicePolicies(org string) {
	pe.ServicePolicies = make(map[string]*ServicePolicyEntry, 0)
}

func (p *BusinessPolicyEntry) UpdateEntry(pol *businesspolicy.BusinessPolicy, polId string, newHash []byte) (*policy.Policy, error) {
	p.Hash = newHash
	p.Updated = uint64(time.Now().Unix())
	p.ServicePolicies = make(map[string]*ServicePolicyEntry, 0)

	// validate and convert the exchange business policy to internal policy format
	if err := pol.Validate(); err != nil {
		return nil, fmt.Errorf("Failed to validate the deployment policy %v. %v", *pol, err)
	} else if pPolicy, err := pol.GenPolicyFromBusinessPolicy(polId); err != nil {
		return nil, fmt.Errorf("Failed to convert the deployment policy to internal policy format: %v. %v", *pol, err)
	} else {
		p.Policy = pPolicy
		return pPolicy, nil
	}
}

type BusinessPolicyManager struct {
	spMapLock      sync.Mutex                                 // The lock that protects the map of ServedPolicies because it is referenced from another thread.
	polMapLock     sync.Mutex                                 // The lock that protects the map of BusinessPolicyEntry because it is referenced from another thread.
	eventChannel   chan events.Message                        // for sending policy change messages
	ServedPolicies map[string]exchange.ServedBusinessPolicy   // served node org, business policy org and business policy triplets. The key is the triplet exchange id.
	OrgPolicies    map[string]map[string]*BusinessPolicyEntry // all served policies by this agbot. The first key is org, the second key is business policy exchange id without org.
}

func (pm *BusinessPolicyManager) String() string {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	res := "Deployment Policy Manager:\n"
	for org, orgMap := range pm.OrgPolicies {
		res += fmt.Sprintf("Org: %v ", org)
		for pat, pe := range orgMap {
			res += fmt.Sprintf("Deployment policy: %v %v \n", pat, pe)
		}
	}

	pm.spMapLock.Lock()
	defer pm.spMapLock.Unlock()

	for _, served := range pm.ServedPolicies {
		res += fmt.Sprintf("Serve: %v ", served)
	}
	return res
}

func (pm *BusinessPolicyManager) ShortString() string {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	res := "Policy Manager: "
	for org, orgMap := range pm.OrgPolicies {
		res += fmt.Sprintf("Org: %v ", org)
		for pat, pe := range orgMap {
			s := ""
			if pe != nil {
				s = pe.ShortString()
			}
			res += fmt.Sprintf("Deployment policy: %v %v ", pat, s)
		}
	}
	return res
}

func NewBusinessPolicyManager(eventChannel chan events.Message) *BusinessPolicyManager {
	pm := &BusinessPolicyManager{
		OrgPolicies:  make(map[string]map[string]*BusinessPolicyEntry),
		eventChannel: eventChannel,
	}
	return pm
}

// New methods to support the IPolicyManager interface.
func (pm *BusinessPolicyManager) NumberPolicies() int {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	num := 0
	for _, polMap := range pm.OrgPolicies {
		num = num + len(polMap)
	}
	return num
}

func (pm *BusinessPolicyManager) GetAllAgreementProtocols() map[string]policy.BlockchainList {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	protocols := make(map[string]policy.BlockchainList)
	for _, polMap := range pm.OrgPolicies {
		for _, pol := range polMap {
			for _, agp := range pol.Policy.AgreementProtocols {
				protocols[agp.Name] = agp.Blockchains
			}
		}
	}
	return protocols
}

// New methods to support the IPolicyManager interface.

func (pm *BusinessPolicyManager) hasOrg(org string) bool {
	_, ok := pm.OrgPolicies[org]
	return ok
}

func (pm *BusinessPolicyManager) hasBusinessPolicy(org string, polName string) bool {
	if pm.hasOrg(org) {
		_, ok := pm.OrgPolicies[org][polName]
		return ok
	}
	return false
}

// Not Used
// func (pm *BusinessPolicyManager) GetAllBusinessPolicyEntriesForOrg(org string) map[string]*BusinessPolicyEntry {
// 	pm.polMapLock.Lock()
// 	defer pm.polMapLock.Unlock()

// 	if pm.hasOrg(org) {
// 		// TODO: This in unsafe fix it.
// 		return pm.OrgPolicies[org]
// 	}
// 	return nil
// }

// Returns a safe array of all the business policies in a given org.
func (pm *BusinessPolicyManager) GetAllBusinessPolicyNames(org string) []string {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	var names []string
	if pm.hasOrg(org) {
		names = make([]string, 0, len(pm.OrgPolicies[org]))
		for n, _ := range pm.OrgPolicies[org] {
			names = append(names, n)
		}
	}
	return names
}

// Returns a copy of the Business Policy and its last updated time.
func (pm *BusinessPolicyManager) GetBusinessPolicy(org string, polName string) (*policy.Policy, uint64) {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	if pm.hasBusinessPolicy(org, polName) {
		bpe := pm.OrgPolicies[org][polName]
		polCopy := bpe.Policy.DeepCopy()
		return polCopy, bpe.Updated
	}
	return nil, 0
}

// Return a safe copy of the list of orgs whose deployment policy is being served by this Agbot.
func (pm *BusinessPolicyManager) GetAllPolicyOrgs() []string {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	orgs := make([]string, 0, len(pm.OrgPolicies))
	for org, _ := range pm.OrgPolicies {
		orgs = append(orgs, org)
	}
	return orgs
}

// copy the given map of served business policies
func (pm *BusinessPolicyManager) setServedBusinessPolicies(servedPols map[string]exchange.ServedBusinessPolicy) {
	pm.spMapLock.Lock()
	defer pm.spMapLock.Unlock()

	// copy the input map
	// TODO: this might not be safe
	pm.ServedPolicies = servedPols
}

// Check if the agbot serves the deployment policy.
func (pm *BusinessPolicyManager) serveBusinessPolicy(polOrg string, polName string) bool {
	pm.spMapLock.Lock()
	defer pm.spMapLock.Unlock()

	for _, sp := range pm.ServedPolicies {
		if sp.BusinessPolOrg == polOrg && (sp.BusinessPol == polName || sp.BusinessPol == "*") {
			return true
		}
	}
	return false
}

// check if the agbot service the given org or not.
func (pm *BusinessPolicyManager) serveOrg(polOrg string) bool {
	pm.spMapLock.Lock()
	defer pm.spMapLock.Unlock()

	for _, sp := range pm.ServedPolicies {
		if sp.BusinessPolOrg == polOrg {
			return true
		}
	}
	return false
}

// return an array of node orgs for the given served policy org and policy.
// this function is called from a different thread.
func (pm *BusinessPolicyManager) GetServedNodeOrgs(polOrg string, polName string) []string {
	pm.spMapLock.Lock()
	defer pm.spMapLock.Unlock()

	node_orgs := []string{}
	for _, sp := range pm.ServedPolicies {
		if sp.BusinessPolOrg == polOrg && (sp.BusinessPol == polName || sp.BusinessPol == "*") {
			node_org := sp.NodeOrg
			// the default node org is the policy org
			if node_org == "" {
				node_org = sp.BusinessPolOrg
			}
			node_orgs = append(node_orgs, node_org)
		}
	}
	return node_orgs
}

// Given a list of policy_org/policy/node_org triplets that this agbot is supposed to serve, save that list and
// convert it to map of maps (keyed by org and policy name) to hold all the policy meta data. This
// will allow the BusinessPolicyManager to know when the policy metadata changes.

//func (pm *BusinessPolicyManager) SetCurrentBusinessPolicies(servedPols map[string]exchange.ServedBusinessPolicy, polManager *policy.PolicyManager) error {
func (pm *BusinessPolicyManager) SetCurrentBusinessPolicies(servedPols map[string]exchange.ServedBusinessPolicy) error {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	// Exit early if nothing to do.
	if len(pm.ServedPolicies) == 0 && len(servedPols) == 0 {
		return nil
	}

	// Save the served business policies.
	pm.setServedBusinessPolicies(servedPols)

	// If this is the first time to update served policies, create a new map of maps.
	if len(pm.OrgPolicies) == 0 {
		pm.OrgPolicies = make(map[string]map[string]*BusinessPolicyEntry)
	}

	// For each org that this agbot is supposed to be serving, check if it is already in the pm.
	// If not add to it. The policies will be added later in the UpdatePolicies function.
	for _, served := range servedPols {
		// If we have encountered a new org in the served policy list, create a map of policies for it.
		if !pm.hasOrg(served.BusinessPolOrg) {
			pm.OrgPolicies[served.BusinessPolOrg] = make(map[string]*BusinessPolicyEntry)
		}
	}

	// For each org in the existing BusinessPolicyManager, check to see if its in the new map. If not, then
	// this agbot is no longer serving any business polices in that org, we can get rid of everything in that org.
	for org, _ := range pm.OrgPolicies {
		if !pm.serveOrg(org) {
			glog.V(5).Infof("Deleting the org %v from the dpoloyment policy manager because it is no longer served by the agbot.", org)
			if err := pm.deleteOrg(org); err != nil {
				return err
			}
		}
	}

	return nil
}

// For each org that the agbot is supporting, take the set of deployment policies defined within the org and save them into
// the BusinessPolicyManager. When new or updated policies are discovered, clear ServicePolicies for that BusinessPolicyEntry so that
// new deployment polices can be filled later.
func (pm *BusinessPolicyManager) UpdatePolicies(org string, definedPolicies map[string]exchange.ExchangeBusinessPolicy) error {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	// Exit early on error
	if !pm.hasOrg(org) {
		glog.Infof("org %v not found in policy manager", org)
		return nil
	}

	// If there is no business policy in the org, delete the org from the pm.
	// This is the case where business policy or the org has been deleted but the agbot still hosts the policy on the exchange.
	if definedPolicies == nil || len(definedPolicies) == 0 {
		// delete org and all policy files in it.
		glog.V(5).Infof("Deleting org %v from the deployment policy manager because it does not contain a business policy.", org)
		return pm.deleteOrg(org)
	}

	// Delete the deployment policy if it does not exist on the exchange or the agbot
	// does not serve it any more.
	for polName, _ := range pm.OrgPolicies[org] {
		need_delete := true
		if pm.serveBusinessPolicy(org, polName) {
			for polId, _ := range definedPolicies {
				if exchange.GetId(polId) == polName {
					need_delete = false
					break
				}
			}
		}

		if need_delete {
			glog.V(5).Infof("Deleting deployment policy %v/%v because the policy no longer exists.", org, polName)
			if err := pm.deleteBusinessPolicy(org, polName); err != nil {
				glog.Errorf("Error deleting deployment policy %v/%v, error: %v", org, polName, err)
				continue
			}
		}
	}

	// Now we just need to handle adding new business policies or update existing business policies
	for polId, exPol := range definedPolicies {
		pol := exPol.GetBusinessPolicy()
		if !pm.serveBusinessPolicy(org, exchange.GetId(polId)) {
			continue
		}
		if err := pm.UpdateBusinessPolicy(org, polId, &pol); err != nil {
			glog.Errorf("Error updating deployment policy %v/%v, error: %v", org, polId, err)
			continue
		}
	}

	return nil
}

// Add or update the given deployment policy. If the policy has changed, send an event to notify the other workers.
func (pm *BusinessPolicyManager) UpdateBusinessPolicy(org string, polId string, pol *businesspolicy.BusinessPolicy) error {
	need_new_entry := true
	if pm.hasBusinessPolicy(org, exchange.GetId(polId)) {
		if pe := pm.OrgPolicies[org][exchange.GetId(polId)]; pe != nil {
			need_new_entry = false

			// The PolicyEntry is already there, so check if the policy definition has changed.
			// If the policy has changed, Send a PolicyChangedMessage message. Otherwise the policy
			// definition we have is current.
			newHash, err := cutil.HashObject(pol)
			if err != nil {
				return errors.New(fmt.Sprintf("unable to hash the deployment policy %v/%v, error %v", org, pol, err))
			}
			if !bytes.Equal(pe.Hash, newHash) {
				// Update the cache.
				glog.V(5).Infof("Updating policy entry for %v/%v because it is changed.", org, polId)
				newPol, err := pe.UpdateEntry(pol, polId, newHash)
				if err != nil {
					return errors.New(fmt.Sprintf("error updating deployment policy entry for %v/%v, error %v", org, polId, err))
				}

				// // notify the policy manager
				// polManager.UpdatePolicy(org, newPol)

				// Send a message so that other workers can handle it.
				glog.V(3).Infof(fmt.Sprintf("Deployment policy manager detected changed deployment policy %v/%v", org, polId))
				if policyString, err := policy.MarshalPolicy(newPol); err != nil {
					glog.Errorf(fmt.Sprintf("Error trying to marshal policy %v error: %v", newPol, err))
				} else {
					pm.eventChannel <- events.NewPolicyChangedMessage(events.CHANGED_POLICY, "", newPol.Header.Name, org, policyString)
				}
			}
		}
	}

	//If there's no BusinessPolicyEntry yet, create one
	if need_new_entry {
		if newPE, err := NewBusinessPolicyEntry(pol, polId); err != nil {
			return errors.New(fmt.Sprintf("unable to create deployment policy entry for %v/%v, error %v", org, polId, err))
		} else {
			pm.OrgPolicies[org][exchange.GetId(polId)] = newPE

			// // notify the policy manager
			// polManager.AddPolicy(org, newPE.Policy)
		}
	}

	return nil
}

// When an org is removed from the list of supported orgs and deployment policies, remove the org
// from the BusinessPolicyManager.

//func (pm *BusinessPolicyManager) deleteOrg(org_in string, polManager *policy.PolicyManager) error {
func (pm *BusinessPolicyManager) deleteOrg(org_in string) error {
	// send PolicyDeletedMessage message for each business polices in the org
	for org, orgMap := range pm.OrgPolicies {
		if org == org_in {
			for polName, pe := range orgMap {
				if pe != nil {
					glog.V(3).Infof(fmt.Sprintf("Deployment policy manager detected deleted policy %v", polName))

					// // notify the policy manager
					// polManager.DeletePolicy(org, pe.Policy)

					if policyString, err := policy.MarshalPolicy(pe.Policy); err != nil {
						glog.Errorf(fmt.Sprintf("Deployment policy manager error trying to marshal policy %v error: %v", polName, err))
					} else {
						pm.eventChannel <- events.NewPolicyDeletedMessage(events.DELETED_POLICY, "", pe.Policy.Header.Name, org, policyString)
					}
				}
			}
			break
		}
	}

	// Get rid of the org map
	if pm.hasOrg(org_in) {
		delete(pm.OrgPolicies, org_in)
	}
	return nil
}

// When a business policy is removed from the exchange, remove it from the BusinessPolicyManager, PolicyManager and send a PolicyDeletedMessage.
func (pm *BusinessPolicyManager) deleteBusinessPolicy(org string, polName string) error {
	// Get rid of the business policy from the pm
	if pm.hasOrg(org) {
		if pe, ok := pm.OrgPolicies[org][polName]; ok {
			if pe != nil {
				glog.V(3).Infof(fmt.Sprintf("Policy manager detected deleted policy %v", polName))

				// // notify the policy manager
				// polManager.DeletePolicy(org, pe.Policy)

				if policyString, err := policy.MarshalPolicy(pe.Policy); err != nil {
					glog.Errorf(fmt.Sprintf("Policy manager error trying to marshal policy %v error: %v", polName, err))
				} else {
					pm.eventChannel <- events.NewPolicyDeletedMessage(events.DELETED_POLICY, "", pe.Policy.Header.Name, org, policyString)
				}
			}

			delete(pm.OrgPolicies[org], polName)
		}
	}

	return nil
}

// Return all cached service policies for a business policy
func (pm *BusinessPolicyManager) GetServicePoliciesForPolicy(org string, polName string) map[string]externalpolicy.ExternalPolicy {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	svcPols := make(map[string]externalpolicy.ExternalPolicy, 0)
	if pm.hasOrg(org) {
		if entry, ok := pm.OrgPolicies[org][polName]; ok {
			if entry != nil && entry.ServicePolicies != nil {
				for svcId, svcPolEntry := range entry.ServicePolicies {
					svcPols[svcId] = *svcPolEntry.Policy
				}
			}
		}
	}
	return svcPols
}

// Add or update the given marshaled service policy.
func (pm *BusinessPolicyManager) AddMarshaledServicePolicy(businessPolOrg, businessPolName, serviceId, servicePolString string) error {

	servicePol := new(externalpolicy.ExternalPolicy)
	if err := json.Unmarshal([]byte(servicePolString), servicePol); err != nil {
		return fmt.Errorf("Failed to unmashling the given service policy for service %v. %v", serviceId, err)
	}

	return pm.AddServicePolicy(businessPolOrg, businessPolName, serviceId, servicePol)
}

// Add or update the given service policy in all needed business policy entries. Send a message for each business policy if it is updating so that
// the event handler can reevaluating the agreements.
func (pm *BusinessPolicyManager) AddServicePolicy(businessPolOrg string, businessPolName string, serviceId string, servicePol *externalpolicy.ExternalPolicy) error {

	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	orgMap, found := pm.OrgPolicies[businessPolOrg]
	if !found {
		return fmt.Errorf("No business polices found under org %v.", businessPolOrg)
	}

	pBE, found := orgMap[businessPolName]
	if !found {
		return fmt.Errorf("Cannnot find cached business policy %v/%v", businessPolOrg, businessPolName)
	}

	policyString := ""

	if updated, err := pBE.AddServicePolicy(servicePol, serviceId); err != nil {
		return fmt.Errorf("Failed to add service policy for service %v to the policy manager. %v", serviceId, err)
	} else {
		if updated {
			// send an event for service policy changed
			if polTemp, err := json.Marshal(servicePol); err != nil {
				return fmt.Errorf("Policy manager error trying to marshal service policy for service %v error: %v", serviceId, err)
			} else {
				policyString = string(polTemp)
			}
			pm.eventChannel <- events.NewServicePolicyChangedMessage(events.SERVICE_POLICY_CHANGED, businessPolOrg, businessPolName, serviceId, policyString)
		}

		// check if there are other business policies using the same service policy, we need to update them too
		for org, orgMap := range pm.OrgPolicies {
			if orgMap == nil {
				continue
			}
			for bpName, pbe := range orgMap {
				// this is the one that's just handled, skip it
				if bpName == businessPolName && org == businessPolOrg {
					continue
				}
				if pbe.ServicePolicies == nil {
					continue
				}
				for sId, _ := range pbe.ServicePolicies {
					if sId == serviceId {
						if updated, err := pbe.AddServicePolicy(servicePol, serviceId); err != nil {
							return fmt.Errorf("Failed to update service policy for service %v to the policy manager. %v", serviceId, err)
						} else if updated {
							// send an event for service policy changed
							if policyString == "" {
								if polTemp, err := json.Marshal(servicePol); err != nil {
									return fmt.Errorf("Policy manager error trying to marshal service policy for service %v error: %v", serviceId, err)
								} else {
									policyString = string(polTemp)
								}
							}
							pm.eventChannel <- events.NewServicePolicyChangedMessage(events.SERVICE_POLICY_CHANGED, org, bpName, serviceId, policyString)
						}
					}
				}
			}
		}

		return nil
	}

}

// Delete the given service policy in all the business policy entries. Send a message for each business policy so that
// the event handler can re-evaluating the agreements.
func (pm *BusinessPolicyManager) RemoveServicePolicy(businessPolOrg string, businessPolName string, serviceId string) error {

	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	orgMap, found := pm.OrgPolicies[businessPolOrg]
	if !found {
		return nil
	}

	pBE, found := orgMap[businessPolName]
	if !found {
		return nil
	}

	if removed := pBE.RemoveServicePolicy(serviceId); removed {
		pm.eventChannel <- events.NewServicePolicyDeletedMessage(events.SERVICE_POLICY_DELETED, businessPolOrg, businessPolName, serviceId)
	}

	// check if there are other business policies using the samve service policy, we need to update them too
	for org, orgMap := range pm.OrgPolicies {
		if orgMap == nil {
			continue
		}
		for bpName, pbe := range orgMap {
			// this is the one that's just handled, skip it
			if bpName == businessPolName && org == businessPolOrg {
				continue
			}
			if pbe.ServicePolicies == nil {
				continue
			}
			for sId, _ := range pbe.ServicePolicies {
				if sId == serviceId {
					if removed := pbe.RemoveServicePolicy(serviceId); removed {
						pm.eventChannel <- events.NewServicePolicyDeletedMessage(events.SERVICE_POLICY_DELETED, businessPolOrg, businessPolName, serviceId)
					}
				}
			}
		}
	}
	return nil
}
