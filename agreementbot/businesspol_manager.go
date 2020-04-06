package agreementbot

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/open-horizon/anax/businesspolicy"
	"github.com/open-horizon/anax/cutil"
	"github.com/open-horizon/anax/exchange"
	"github.com/open-horizon/anax/externalpolicy"
	"github.com/open-horizon/anax/policy"
	"sync"
)

type BusinessPolicyManager struct {
	spMapLock  sync.Mutex // The lock that protects the map of ServedPolicies because it is referenced from another thread.
	polMapLock sync.Mutex // The lock that protects the map of BusinessPolicyEntry because it is referenced from another thread.
	// eventChannel   chan events.Message                        // for sending policy change messages
	ServedPolicies map[string]exchange.ServedBusinessPolicy   // Served node org, deployment policy org and deployment policy triplets. The key is the triplet exchange id.
	OrgPolicies    map[string]map[string]*BusinessPolicyEntry // All deployment policies served by this agbot. The first key is org, the second key is deployment policy exchange id without org.
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

func NewBusinessPolicyManager() *BusinessPolicyManager {
	pm := &BusinessPolicyManager{
		OrgPolicies: make(map[string]map[string]*BusinessPolicyEntry),
	}
	return pm
}

// Try to delete these.
// ======================================================================
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
// ======================================================================

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

func (pm *BusinessPolicyManager) getBusinessPolicyEntry(org string, polName string) *BusinessPolicyEntry {
	if pm.hasBusinessPolicy(org, polName) {
		return pm.OrgPolicies[org][polName]
	}
	return nil
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

// Returns a safe copy of the Deployment Policy in the internal form, and its last updated time.
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
		if _, err := pm.updateBusinessPolicy(org, polId, &pol); err != nil {
			glog.Errorf("Error updating deployment policy %v/%v, error: %v", org, polId, err)
			continue
		}
	}

	return nil
}

// External function to add or update the given deployment policy. The deployment policy cache lock is obtained and then
// the internal equivalent function is called. Returns true if the new deployment policy is actually changed.
func (pm *BusinessPolicyManager) UpdateBusinessPolicy(org string, polId string, pol *businesspolicy.BusinessPolicy) (bool, error) {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()
	return pm.updateBusinessPolicy(org, polId, pol)
}

// Add or update the given deployment policy. This is an internal function that assumes the deployment policy cache lock
// is already held.
func (pm *BusinessPolicyManager) updateBusinessPolicy(org string, polId string, pol *businesspolicy.BusinessPolicy) (bool, error) {

	if pe := pm.getBusinessPolicyEntry(org, exchange.GetId(polId)); pe != nil {

		// The PolicyEntry is already there, so check if the policy definition has changed.
		if newHash, err := cutil.HashObject(pol); err != nil {
			return false, errors.New(fmt.Sprintf("unable to hash the deployment policy %v, error %v", pol, err))
		} else if !bytes.Equal(pe.Hash, newHash) {
			// Update the cache.
			glog.V(5).Infof("Updating policy entry for %v because it is changed.", polId)
			if _, err := pe.UpdateEntry(pol, polId, newHash); err != nil {
				return false, errors.New(fmt.Sprintf("error updating deployment policy entry for %v, error %v", polId, err))
			} else {
				return true, nil
			}

			// // notify the policy manager
			// polManager.UpdatePolicy(org, newPol)

			// Send a message so that other workers can handle it.
			// glog.V(3).Infof(fmt.Sprintf("Deployment policy manager detected changed deployment policy %v/%v", org, polId))
			// if policyString, err := policy.MarshalPolicy(newPol); err != nil {
			// 	glog.Errorf(fmt.Sprintf("Error trying to marshal policy %v error: %v", newPol, err))
			// } else {
			// 	pm.eventChannel <- events.NewPolicyChangedMessage(events.CHANGED_POLICY, "", newPol.Header.Name, org, policyString)
			// }
		}

	} else if newPE, err := NewBusinessPolicyEntry(pol, polId); err != nil {
		return false, errors.New(fmt.Sprintf("unable to create deployment policy entry for %v/%v, error %v", org, polId, err))
	} else {
		// Update the deployment policy cache with the new deployment policy entry.
		pm.OrgPolicies[org][exchange.GetId(polId)] = newPE
		glog.V(3).Infof(fmt.Sprintf("Deployment policy manager detected new deployment policy %v/%v", org, polId))

		// // notify the policy manager
		// polManager.AddPolicy(org, newPE.Policy)
	}

	return true, nil
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

					// TODO: Figure out a better way than this to handled the removal of an org form the served list.

					// if policyString, err := policy.MarshalPolicy(pe.Policy); err != nil {
					// 	glog.Errorf(fmt.Sprintf("Deployment policy manager error trying to marshal policy %v error: %v", polName, err))
					// } else {
					// 	pm.eventChannel <- events.NewPolicyDeletedMessage(events.DELETED_POLICY, "", pe.Policy.Header.Name, org, policyString)
					// }
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

// External function to delete the given deployment policy. The deployment policy cache lock is obtained and then
// the internal equivalent function is called.
func (pm *BusinessPolicyManager) DeleteBusinessPolicy(org string, polId string) error {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()
	return pm.deleteBusinessPolicy(org, polId)
}

// When a business policy is removed from the exchange, remove it from the BusinessPolicyManager, PolicyManager and send a PolicyDeletedMessage.
func (pm *BusinessPolicyManager) deleteBusinessPolicy(org string, polName string) error {
	// Get rid of the deployment policy from the cache.
	if pe := pm.getBusinessPolicyEntry(org, polName); pe != nil {

		glog.V(3).Infof(fmt.Sprintf("Deployment policy manager detected deleted policy %v/%v", org, polName))

		// // notify the policy manager
		// polManager.DeletePolicy(org, pe.Policy)

		// if policyString, err := policy.MarshalPolicy(pe.Policy); err != nil {
		// 	glog.Errorf(fmt.Sprintf("Policy manager error trying to marshal policy %v error: %v", polName, err))
		// } else {
		// 	pm.eventChannel <- events.NewPolicyDeletedMessage(events.DELETED_POLICY, "", pe.Policy.Header.Name, org, policyString)
		// }

		delete(pm.OrgPolicies[org], polName)
	}

	return nil
}

// Return all cached service policies for a business policy
func (pm *BusinessPolicyManager) GetServicePoliciesForPolicy(org string, polName string) map[string]externalpolicy.ExternalPolicy {
	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	svcPols := make(map[string]externalpolicy.ExternalPolicy, 0)
	// if pm.hasOrg(org) {
	// 	if entry, ok := pm.OrgPolicies[org][polName]; ok {
	if pm.hasBusinessPolicy(org, polName) {
		if entry := pm.getBusinessPolicyEntry(org, polName); entry != nil && entry.ServicePolicies != nil {
			for svcId, svcPolEntry := range entry.ServicePolicies {
				// TODO: make sure these are safe copies to return.
				svcPols[svcId] = *svcPolEntry.Policy
			}
			// }
		}
	}
	return svcPols
}

// // Add or update the given marshaled service policy.
// func (pm *BusinessPolicyManager) AddMarshaledServicePolicy(businessPolOrg, businessPolName, serviceId, servicePolString string) error {

// 	servicePol := new(externalpolicy.ExternalPolicy)
// 	if err := json.Unmarshal([]byte(servicePolString), servicePol); err != nil {
// 		return fmt.Errorf("Failed to unmashling the given service policy for service %v. %v", serviceId, err)
// 	}

// 	return pm.AddServicePolicy(businessPolOrg, businessPolName, serviceId, servicePol)
// }

// Add or update the given service policy in all needed business policy entries. Send a message for each business policy if it is updating so that
// the event handler can reevaluating the agreements.
func (pm *BusinessPolicyManager) AddServicePolicy(businessPolOrg string, businessPolName string, serviceId string, servicePol *externalpolicy.ExternalPolicy) error {

	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	// Verify that the service policy belongs to a deployment policy that is known.
	if !pm.hasOrg(businessPolOrg) {
		return fmt.Errorf("No deployment polices found under org %v.", businessPolOrg)
	}

	if !pm.hasBusinessPolicy(businessPolOrg, businessPolName) {
		return fmt.Errorf("Cannnot find cached deployment policy %v/%v", businessPolOrg, businessPolName)
	}

	// Get the deployment policy cache entry for the input deployment policy. This is where the service policies are cached.
	pBE := pm.getBusinessPolicyEntry(businessPolOrg, businessPolName)
	// policyString := ""

	if _, err := pBE.AddServicePolicy(servicePol, serviceId); err != nil {
		return fmt.Errorf("Failed to add service policy for service %v to the policy manager, error %v", serviceId, err)
	} else {
		// if updated {
		// 	// send an event for service policy changed
		// 	if polTemp, err := json.Marshal(servicePol); err != nil {
		// 		return fmt.Errorf("Policy manager error trying to marshal service policy for service %v, error %v", serviceId, err)
		// 	} else {
		// 		policyString = string(polTemp)
		// 	}
		// 	pm.eventChannel <- events.NewServicePolicyChangedMessage(events.SERVICE_POLICY_CHANGED, businessPolOrg, businessPolName, serviceId, policyString)
		// }

		// If there are other deployment policies using the same service policy, update them too.
		// TODO: restructure this
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
						if _, err := pbe.AddServicePolicy(servicePol, serviceId); err != nil {
							return fmt.Errorf("Failed to update service policy for service %v to the policy manager. %v", serviceId, err)
						}
						// } else if updated {
						// // send an event for service policy changed
						// if policyString == "" {
						// 	if polTemp, err := json.Marshal(servicePol); err != nil {
						// 		return fmt.Errorf("Policy manager error trying to marshal service policy for service %v error: %v", serviceId, err)
						// 	} else {
						// 		policyString = string(polTemp)
						// 	}
						// }
						// pm.eventChannel <- events.NewServicePolicyChangedMessage(events.SERVICE_POLICY_CHANGED, org, bpName, serviceId, policyString)
						// }
					}
				}
			}
		}

		return nil
	}

}

// Update a service policy in the cache (because it changed) and return a list of deployment policy Ids that are known to be
// using this service policy. The input serviceId is a fully qualified is (i.e. prefixed with the service org). It is
// possible that the input service id is not cached in the policy manager. That is not an error, only the service policies
// used by deployment policies to for agreements are cached here.
func (pm *BusinessPolicyManager) UpdateServicePolicy(serviceId string, servicePol *externalpolicy.ExternalPolicy) ([]string, error) {

	pm.polMapLock.Lock()
	defer pm.polMapLock.Unlock()

	depPolIds := make([]string, 0, 10)
	rememberErr := error(nil)

	// Iterate through every deployment policy org, and every deployment policy entry to find all deployment
	// policies which are using this service policy.
	for org, orgMap := range pm.OrgPolicies {
		if orgMap == nil {
			continue
		}

		for depName, pbe := range orgMap {
			spe := pbe.getServicePolicy(serviceId)
			if spe == nil {
				continue
			} else {
				// The input service policy is cached here. Try to update it. If it is different, updated will be
				// true. In this case, add the related deployment policy to the list of returned policies. Errors
				// encountered along the way are remembered and returned, but note that the function will still
				// update the cached service policies as much as possible.
				updated, err := spe.UpdateServicePolicyEntry(servicePol)
				if err != nil {
					rememberErr = err
				}
				if updated {
					depPolIds = append(depPolIds, fmt.Sprintf("%v/%v", org, depName))
				}
			}

		}
	}

	return depPolIds, rememberErr

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

	// TODO: Consolidate this into 1 loop
	pBE.RemoveServicePolicy(serviceId)

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
					pbe.RemoveServicePolicy(serviceId)
				}
			}
		}
	}
	return nil
}
